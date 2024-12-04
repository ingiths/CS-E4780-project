use chrono::{TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TickEvent {
    pub last: Option<f64>,
    pub trading_timestamp: Option<i64>,
    pub id: String,
    pub equity_type: String,
}

impl TickEvent {
    pub fn is_valid(&self) -> bool {
        if self.last.is_none() || self.trading_timestamp.is_none() {
            return false;
        }

        let result = match self.trading_timestamp {
            Some(t) => {
                let trading_timestamp = Utc.timestamp_millis_opt(t).unwrap();
                if trading_timestamp.hour() == 0
                    && trading_timestamp.minute() == 0
                    && trading_timestamp.second() == 0
                {
                    return false;
                }
                return true;
            }
            None => false,
        };

        result
    }
}
