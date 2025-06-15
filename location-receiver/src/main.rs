use ::time::OffsetDateTime;
use env_logger::Env;
use std::io::{BufRead, Write};
use std::net::TcpListener;
use std::path::Path;
use std::thread;
use std::time::SystemTime;

use common::buffer::BufReaderDirectWriter;

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    log::info!("location-receiver starting up");

    let db_path = Path::new("/var/db");
    std::fs::create_dir_all(&db_path).expect("Cannot create /var/db directory");

    let listener = TcpListener::bind("0.0.0.0:11328").expect("Cannot bind to port 11328");

    loop {
        let (stream, addr) = listener.accept().expect("Failed to accept connection");
        log::info!("Accepted connection from: {}", addr);
        thread::spawn(move || {
            let mut buffer = String::new();
            let mut reader = BufReaderDirectWriter::new(stream);
            loop {
                match reader.read_line(&mut buffer) {
                    Ok(0) => break, // Connection closed
                    Ok(_) => {
                        log::info!("Received message: {}", buffer);
                        // Process the message here
                        for line in buffer.lines() {
                            if !line.is_empty() {
                                process_message(line, &db_path);
                            }
                        }
                        buffer.clear();
                    }
                    Err(e) => {
                        log::error!("Error reading from stream: {}", e);
                        break;
                    }
                }
            }
        });
    }
}

fn process_message(message: &str, db_path: &Path) {
    // Parse the message and handle it accordingly
    let i = message.find('$').unwrap_or(0);
    if i == 0 {
        log::error!("No MMSI/boatname in '{}'", message);
        return;
    }
    let (id, message) = message.split_at(i);
    let nmea_id = &message[3..6].to_lowercase();
    let message = format!("{}\r\n", message);
    let now: OffsetDateTime = SystemTime::now().into();
    let year = now.year();

    let path = db_path.join(format!("{}_{}_{}.db", id, year, nmea_id));
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("Cannot open database file");
    if let Err(e) = file.write_all(message.as_bytes()) {
        log::error!("Error writing to {}: {}", path.display(), e);
    }
}
