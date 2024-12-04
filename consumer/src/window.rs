use std::collections::HashMap;

use crate::influx::{BreakoutType, InfluxResults};
use chrono::Utc;

use crate::tick::TickEvent;

const EMA_38: f32 = 38.0;
const EMA_100: f32 = 100.0;

fn round_down(number: i64, multiplier: i64) -> i64 {
    ((number + multiplier / 2) / multiplier) * multiplier
}


#[derive(Clone)]
struct EMA {
    ema_38: f32,
    ema_100: f32,
}

impl EMA {
    fn new() -> EMA {
        EMA {
            ema_38: 0.0,
            ema_100: 0.0,
        }
    }

    fn calc(&mut self, last_price: f32, previous: (f32, f32)) -> (f32, f32) {
        self.ema_38 =
            last_price * (2.0 / (1.0 + EMA_38)) + previous.0 * (1.0 - 2.0 / (1.0 + EMA_38));
        self.ema_100 =
            last_price * (2.0 / (1.0 + EMA_100)) + previous.1 * (1.0 - 2.0 / (1.0 + EMA_100));
        (self.ema_38, self.ema_100)
    }
}

#[derive(Clone)]
pub struct Window {
    ema: EMA,
    pub sequence_number: u32,
    pub current_emas: (f32, f32), // (ema_38, ema_100)
    pub start_time: i64,
    pub end_time: i64,
    // Information about prices
    pub first: f32, // Price of window opening
    pub last: f32,  // Price of window closing
    pub max: f32,   // Max value of window
    pub min: f32,   // Min value of window
    pub movements: u32,
}

impl Window {
    fn new(start_time: i64, price: f32) -> Window {
        Window {
            ema: EMA::new(),
            sequence_number: 0,
            // (ema_38, ema_100)
            current_emas: (0.0, 0.0),
            start_time,
            end_time: start_time + 300 * 1000,
            first: price,
            last: 0.0,
            max: price,
            min: price,
            movements: 0,
        }
    }

    fn tumble(
        &mut self,
        new_start_time: i64,
        last_price: f32,
    ) -> Option<(BreakoutType, (f32, f32))> {
        // (ema_38, ema_100)
        let (new_ema_38, new_ema_100) = self.ema.calc(last_price, self.current_emas);
        let (old_ema_38, old_ema_100) = self.current_emas;
        self.start_time = new_start_time;
        self.end_time = self.start_time + 300 * 1000;

        // Update values for candlestick chart
        self.first = last_price;
        self.last = 0.0;
        self.max = last_price;
        self.min = last_price;
        self.movements = 0;

        // A bearish breakout event occurs when:
        // - Current window: EMA_38 < EMA_100
        // - Previous window: EMA_38 >= EMA_100
        // Special case of event when windows start
        self.current_emas = (new_ema_38, new_ema_100);
        if self.sequence_number > 0 {
            let result = if new_ema_38 < new_ema_100 && old_ema_38 >= old_ema_100 {
                Some((BreakoutType::Bearish, (old_ema_38, old_ema_38)))
            } else if new_ema_38 > new_ema_100 && old_ema_38 <= old_ema_100 {
                Some((BreakoutType::Bullish, (old_ema_38, old_ema_100)))
            } else {
                None
            };
            self.sequence_number += 1;
            result
        } else {
            self.sequence_number += 1;
            None
        }
    }
}

pub struct WindowManager {
    windows: HashMap<String, Window>,
}

impl WindowManager {
    pub fn new() -> WindowManager {
        WindowManager {
            windows: HashMap::new(),
        }
    }

    pub fn update(&mut self, tick_event: TickEvent) -> Option<InfluxResults> {
        let trading_timestamp = tick_event
            .trading_timestamp
            .expect("Got invalid tick event");
        let last = tick_event.last.unwrap();

        let window = self
            .windows
            .entry(tick_event.id.clone())
            .or_insert_with(|| Window::new(round_down(trading_timestamp, 300 * 1000), last));

        // Are we receiving events from the past?
        if trading_timestamp < window.start_time {
            return None;
        }

        if window.end_time >= trading_timestamp {
            if window.max < last {
                window.max = last;
            }

            if last < window.min {
                window.min = last;
            }
            window.last = last;
            window.movements += 1;
            return None;
        }

        // Perf event
        let start = Utc::now().timestamp_millis();

        let window_first = window.first;
        let window_last = window.last;
        let window_max = window.max;
        let window_min = window.min;
        let movements = window.movements;
        let window_start_time = window.start_time;

        let breakout = window.tumble(round_down(trading_timestamp as i64, 300 * 1000), last);
        let mut result = InfluxResults::new(
            tick_event.id,
            window_start_time,
            tick_event.equity_type,
            window,
            window_first,
            window_last,
            window_max,
            window_min,
            breakout,
            movements,
        );
        result.record_window_start(start);
        result.record_window_end(Utc::now().timestamp_millis());
        window.last = last;

        Some(result)
    }
}