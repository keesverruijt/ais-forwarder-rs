/// (C) 2025 by Kees Verruijt, Harlingen, Netherlands
use nmea_parser::ParsedMessage;
use std::collections::HashMap;
use std::io;
use std::sync::mpsc::Receiver;
use std::time::Duration;

use crate::cache::Persistence;
use crate::{NetworkEndpoint, send_message};

pub fn work_thread(
    rx: std::sync::mpsc::Receiver<ParsedMessage>,
    location: HashMap<String, NetworkEndpoint>,
    mmsi: u32,
    cache_dir: &str,
) {
    let persistence = Persistence::new(cache_dir);

    let _ = Location::new(location, persistence, mmsi).location_loop(&rx);
}

struct Location {
    location: HashMap<String, NetworkEndpoint>,
    persistence: Persistence,
    mmsi: u32,
}

impl Location {
    fn new(
        location: HashMap<String, NetworkEndpoint>,
        persistence: Persistence,
        mmsi: u32,
    ) -> Self {
        Self {
            location,
            persistence,
            mmsi,
        }
    }

    fn location_loop(&mut self, rx: &Receiver<ParsedMessage>) -> io::Result<()> {
        const MESSAGE_TIMEOUT: Duration = Duration::from_secs(60);

        log::info!(
            "Starting location loop with {} endpoints",
            self.location.len()
        );
        // Keep track of whether we are able to send messages to the server
        let mut connection_ok = self.resend_messages().is_ok();

        loop {
            match rx.recv_timeout(MESSAGE_TIMEOUT) {
                Ok(message) => {
                    log::debug!("Received message: {:?}", message);
                    if !connection_ok {
                        connection_ok = self.resend_messages().is_ok();
                    }
                    connection_ok = self.parse_message(&message, connection_ok).is_ok();
                }
                Err(e) => match e {
                    std::sync::mpsc::RecvTimeoutError::Timeout => {
                        connection_ok = self.resend_messages().is_ok();
                        continue;
                    }
                    std::sync::mpsc::RecvTimeoutError::Disconnected => {
                        log::error!("Receiver disconnected");
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Receiver disconnected",
                        ));
                    }
                },
            }
        }
    }

    fn resend_messages(&mut self) -> io::Result<()> {
        let resend_count = self.persistence.count();
        if resend_count == 0 {
            log::debug!("No messages to resend from persistence");
            return Ok(());
        }
        log::info!("Resending {} messages from persistence", resend_count);
        for item in self.persistence.iter() {
            match item {
                Ok((key, value)) => {
                    let key = &key.to_vec();
                    let value = &value.to_vec();
                    let skey = String::from_utf8_lossy(&key);
                    let svalue = String::from_utf8_lossy(&value);
                    log::debug!("Resending message: {}: {}", skey, svalue);
                    for (key, address) in self.location.iter_mut() {
                        send_message(value, key, address)?;
                    }
                    self.persistence.remove(key);
                    self.persistence.flush();
                }
                Err(e) => {
                    log::error!("Error reading from database: {}", e);
                }
            }
        }
        Ok(())
    }

    fn parse_message(&mut self, message: &ParsedMessage, connection_ok: bool) -> io::Result<()> {
        let now = chrono::Utc::now();
        const TIME_FORMAT: &str = "%H%M%S";
        const DATE_FORMAT: &str = "%d%m%y";

        let nmea_message = match message {
            ParsedMessage::VesselDynamicData(message) => {
                format!(
                    "{}$GNRMC,{},A,{},{},{},{},{},,,A\r\n",
                    message.mmsi,
                    now.format(TIME_FORMAT),
                    Self::format_lat_long(message.latitude, true),
                    Self::format_lat_long(message.longitude, false),
                    "", // Speed over ground,
                    "", // Course over ground,
                    now.format(DATE_FORMAT),
                )
            }
            ParsedMessage::Rmc(message) => {
                let ts = message.timestamp.unwrap_or(now);
                format!(
                    "{}$GNRMC,{},A,{},{},{},{},{},,,A\r\n",
                    self.mmsi,
                    ts.format(TIME_FORMAT),
                    Self::format_lat_long(message.latitude, true),
                    Self::format_lat_long(message.longitude, false),
                    Self::format_option(message.sog_knots),
                    Self::format_option(message.bearing),
                    ts.format(DATE_FORMAT),
                )
            }
            _ => {
                log::warn!("Unsupported message type: {:?}", message);
                return Ok(());
            }
        };

        let nmea_bytes = nmea_message.as_bytes();
        for (key, address) in self.location.iter_mut() {
            let db_key = format!("{}-{}", now, key);
            if !connection_ok {
                log::debug!("Storing message: {}: {}", key, nmea_message);
                self.persistence.store(db_key.as_bytes(), nmea_bytes);
                self.persistence.flush();
            } else {
                log::debug!("Sending message: {}: {}", key, nmea_message);
                if let Err(e) = send_message(&nmea_bytes, key, address) {
                    log::error!("Error sending location message to {}: {}", key, e);
                    self.persistence.store(db_key.as_bytes(), nmea_bytes);
                    self.persistence.flush();
                }
            }
        }
        Ok(())
    }

    fn format_option(value: Option<f64>) -> String {
        match value {
            Some(value) => format!("{:.1}", value),
            None => "".to_string(),
        }
    }

    fn format_lat_long(latlon: Option<f64>, is_lat: bool) -> String {
        match latlon {
            Some(value) => {
                let hemisphere = if is_lat {
                    if value >= 0.0 { "N" } else { "S" }
                } else {
                    if value >= 0.0 { "E" } else { "W" }
                };
                let abs_value = value.abs();
                let degrees = abs_value.trunc();
                let minutes = (abs_value - degrees) * 60.0;
                format!("{:.5},{}", degrees * 100.0 + minutes, hemisphere)
            }
            None => ",".to_string(),
        }
    }
}
