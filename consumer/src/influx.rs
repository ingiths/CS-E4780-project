use chrono::{DateTime, TimeZone, Utc};
use influxdb::{InfluxDbWriteable, WriteQuery};

use crate::Window;

pub enum BreakoutType {
    Bullish,
    Bearish,
}

#[derive(InfluxDbWriteable)]
struct EmaResult {
    time: DateTime<Utc>,
    calc_38: f32,
    calc_100: f32,
    first: f32, // Price of window opening
    last: f32,  // Price of window closing
    max: f32,   // Max value of window
    min: f32,   // Min value of window
    #[influxdb(tag)]
    id: String,
    #[influxdb(tag)]
    equity_type: String,
}

#[derive(InfluxDbWriteable, Debug)]
struct Breakout {
    #[influxdb(tag)]
    id: String,
    #[influxdb(tag)]
    tags: String,
    time: DateTime<Utc>,
    title: String,
}

impl Breakout {
    fn new(
        id: String,
        time: i64,
        btype: BreakoutType,
        previous: (f32, f32),
        current: (f32, f32),
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

#[derive(InfluxDbWriteable, Debug)]
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
    movements: u32
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
            movements
        }
    }

    pub fn into_influx_query(self) -> WriteQuery {
        self.into_query("perf")
    }
}

pub struct InfluxResults {
    ema: EmaResult,
    breakout: Option<Breakout>,
    perf: Performance,
}

impl InfluxResults {
    pub fn new(
        id: String,
        ts: i64,
        equity_type: String,
        window: &Window,
        window_first: f32,
        window_last: f32,
        window_max: f32,
        window_min: f32,
        breakout: Option<(BreakoutType, (f32, f32))>,
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
