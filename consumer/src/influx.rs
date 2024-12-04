use chrono::{DateTime, TimeZone, Utc};
use influxdb::{InfluxDbWriteable, WriteQuery};
use tokio::sync::mpsc::Receiver;
use tokio::time::timeout;

use crate::window::Window;

#[derive(Clone)]
pub struct InfluxConfig {
    pub batch_size: usize,
    pub flush_period: u64,
}

impl InfluxConfig {
    pub fn new(batch_size: usize, flush_period: u64) -> InfluxConfig {
        InfluxConfig {
            batch_size,
            flush_period,
        }
    }
}

pub enum BreakoutType {
    Bullish,
    Bearish,
}

#[derive(InfluxDbWriteable, Clone)]
struct EmaResult {
    time: DateTime<Utc>,
    calc_38: f64,
    calc_100: f64,
    first: f64, // Price of window opening
    last: f64,  // Price of window closing
    max: f64,   // Max value of window
    min: f64,   // Min value of window
    movements: u32,
    #[influxdb(tag)]
    id: String,
    #[influxdb(tag)]
    equity_type: String,
}

#[derive(InfluxDbWriteable, Clone, Debug)]
pub struct Breakout {
    #[influxdb(tag)]
    pub id: String,
    #[influxdb(tag)]
    pub tags: String,
    pub time: DateTime<Utc>,
    title: String,
}

impl Breakout {
    fn new(
        id: String,
        time: i64,
        btype: BreakoutType,
        previous: (f64, f64),
        current: (f64, f64),
    ) -> Breakout {
        let time = Utc.timestamp_millis_opt(time).unwrap();
        let breakout = match btype {
            BreakoutType::Bullish => Breakout {
                id: id.clone(),
                time,
                title: format!("Bullish event (BUY BUY BUY!) for {} due to: WindowCurrent({} > {}) AND WindowPrevious({} <= {})", id, current.0, current.1, previous.0, previous.1),
                tags: "bullish".to_string(),
            },
            BreakoutType::Bearish => Breakout {
                id: id.clone(),
                time,
                title: format!("Bearish event (SELL SELL SELL!) for {} due to WindowCurrent({} < {}) AND WindowPrevious({} >= {})", id, current.0, current.1, previous.0, previous.1),
                tags: "bearish".to_string(),
            },
        };
        breakout
    }
}

#[derive(InfluxDbWriteable, Clone, Debug)]
pub struct Performance {
    #[influxdb(tag)]
    id: String,
    #[influxdb(tag)]
    window_number: u32,
    time: DateTime<Utc>,
    window_creation_start: i64,
    window_creation_end: i64,
    // Invariant: window_creation_end = influx_write_start
    pub influx_write_end: i64,
    movements: u32,
}

impl Performance {
    fn new<T: Into<String>>(id: T, window_number: u32, movements: u32) -> Performance {
        Performance {
            id: id.into(),
            window_number,
            time: Utc::now(),
            window_creation_start: 0,
            window_creation_end: 0,
            influx_write_end: 0,
            movements,
        }
    }

    pub fn into_influx_query(self) -> WriteQuery {
        self.into_query("perf")
    }
}

#[derive(Clone)]
pub struct InfluxResults {
    ema: EmaResult,
    pub breakout: Option<Breakout>,
    perf: Performance,
}

impl InfluxResults {
    pub fn new(
        id: String,
        ts: i64,
        equity_type: String,
        window: &Window,
        window_first: f64,
        window_last: f64,
        window_max: f64,
        window_min: f64,
        breakout: Option<(BreakoutType, (f64, f64))>,
        movements: u32,
    ) -> InfluxResults {
        let perf = Performance::new(id.clone(), window.sequence_number, movements);
        let ema = EmaResult {
            id: id.clone(),
            calc_38: window.current_emas.0,
            calc_100: window.current_emas.1,
            time: Utc.timestamp_millis_opt(ts).unwrap(),
            equity_type,
            first: window_first,
            last: window_last,
            max: window_max,
            min: window_min,
            movements,
        };

        let breakout = match breakout {
            Some(b) => Some(Breakout::new(id, ts, b.0, b.1, window.current_emas)),
            None => None,
        };

        InfluxResults {
            ema,
            breakout,
            perf,
        }
    }

    pub fn record_window_start(&mut self, ts: i64) {
        self.perf.window_creation_start = ts;
    }

    pub fn record_window_end(&mut self, ts: i64) {
        self.perf.window_creation_end = ts;
    }

    pub fn into_query(self) -> (WriteQuery, Option<WriteQuery>, Performance) {
        let ema_query = self.ema.into_query("trading_bucket");
        let breakout_query = match self.breakout {
            Some(b) => Some(b.into_query("breakout")),
            None => None,
        };
        (ema_query, breakout_query, self.perf)
    }
}

pub async fn start_influx_background_writer(
    influx_client: influxdb::Client,
    mut receiver: Receiver<InfluxResults>,
    config: InfluxConfig,
) {
    let mut buffer = Vec::with_capacity(config.batch_size);
    let my_duration = tokio::time::Duration::from_millis(config.flush_period);
    loop {
        while let Ok(i) = timeout(my_duration, receiver.recv()).await {
            if let Some(influx_result) = i {
                buffer.push(influx_result);
                if buffer.len() == config.batch_size {
                    let mut window_writes = Vec::with_capacity(buffer.len());
                    let mut breakout_writes = Vec::with_capacity(buffer.len());
                    let mut perf_writes = Vec::with_capacity(buffer.len());
                    buffer.drain(..).into_iter().for_each(|influx_result| {
                        let (window_write, breakout_write, perf) = influx_result.into_query();
                        window_writes.push(window_write);
                        if let Some(bw) = breakout_write {
                            breakout_writes.push(bw);
                        }
                        perf_writes.push(perf);
                    });
                    influx_client.query(window_writes).await.unwrap();
                    if breakout_writes.len() > 0 {
                        influx_client.query(breakout_writes).await.unwrap();
                    }
                    let write_end = Utc::now().timestamp_millis();
                    let perf_writes = perf_writes
                        .into_iter()
                        .map(|mut p| {
                            // Not good design but not a lot of time left
                            p.influx_write_end = write_end;
                            p.into_influx_query()
                        })
                        .collect::<Vec<WriteQuery>>();
                    influx_client.query(perf_writes).await.unwrap();
                }
            }
        }
        // Flush buffer if not empty
        if !buffer.is_empty() {
            let mut window_writes = Vec::with_capacity(buffer.len());
            let mut breakout_writes = Vec::with_capacity(buffer.len());
            let mut perf_writes = Vec::with_capacity(buffer.len());
            buffer.drain(..).into_iter().for_each(|influx_result| {
                let (window_write, breakout_write, perf) = influx_result.into_query();
                window_writes.push(window_write);
                if let Some(bw) = breakout_write {
                    breakout_writes.push(bw);
                }
                perf_writes.push(perf);
            });
            influx_client.query(window_writes).await.unwrap();
            influx_client.query(breakout_writes).await.unwrap();
            let write_end = Utc::now().timestamp_millis();
            let perf_writes = perf_writes
                .into_iter()
                .map(|mut p| {
                    // Not good design but not a lot of time left
                    p.influx_write_end = write_end;
                    p.into_influx_query()
                })
                .collect::<Vec<WriteQuery>>();
            influx_client.query(perf_writes).await.unwrap();
        }
    }
}
