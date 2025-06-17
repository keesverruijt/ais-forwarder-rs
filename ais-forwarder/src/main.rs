use clap::Parser;
use config::Config;
use env_logger::Env;
use nmea_parser::ParsedMessage;
use std::collections::HashMap;
use std::net::{TcpListener, UdpSocket};
use std::path::PathBuf;
use std::process::exit;
use std::sync::mpsc::Sender;
use std::thread::Builder;
use std::time::{Duration, Instant};
use std::{io, path};

use common::NetworkEndpoint;
use common::Protocol;
use common::buffer::BufReaderDirectWriter;
use common::read_message_tcp;
use common::read_message_udp;
use common::send_message_tcp;
use common::send_message_udp;

mod cache;
mod location;

struct LastSent {
    vessel_dynamic_data: Instant,
    vessel_static_data: Instant,
}

struct Dispatcher {
    provider: NetworkEndpoint,
    ais: HashMap<String, NetworkEndpoint>,
    location_tx: Sender<ParsedMessage>,
    interval: u64,
    location_interval: u64,
    location_anchor_interval: u64,
    nmea_parser: nmea_parser::NmeaParser,
    last_sent: HashMap<u32, LastSent>,
    last_sent_location: Instant,
}

#[derive(Parser, Clone, Debug)]
pub struct Cli {
    #[clap(flatten)]
    pub verbose: clap_verbosity_flag::Verbosity<clap_verbosity_flag::InfoLevel>,

    /// Configuration file (supports .ini, .toml, .json, .yaml) --
    /// If the file is relative, it will be searched in /etc/ais-forwarder or /usr/local/etc/ais-forwarder.
    /// If the file is absolute, it will be used as is.
    #[clap(long, default_value = "config")]
    pub config: String,

    /// Cache directory --
    /// This must be a directory that is writable by the user running the program.
    /// If the directory does not exist, it will be created.
    #[clap(long, default_value = "/usr/local/var/cache/ais-forwarder")]
    pub cache_dir: String,
}

fn main() {
    let cli = Cli::parse();
    let log_level = cli.verbose.log_level_filter();
    let mut logger = env_logger::Builder::from_env(Env::default());
    logger.filter_level(log_level);
    // When running as a procd daemon, the PWD environment variable is not set
    // which can be used to shorten the logging records that already contain the timestamp.
    if std::env::var("PWD").is_err() {
        logger.format_timestamp(None);
    }
    logger.init();

    let mut config_path = PathBuf::from(cli.config);
    if config_path.is_relative() {
        config_path = get_config_dir().join(config_path);
    }
    let config_path = config_path
        .to_str()
        .expect("Cannot convert config path to string");
    log::info!("Loading config from {}", config_path);

    let settings = match Config::builder()
        .add_source(config::File::with_name(config_path))
        .build()
    {
        Ok(config) => config,
        Err(e) => {
            log::error!("Error loading {}: {}", config_path, e);
            exit(1);
        }
    };

    let settings = match settings.try_deserialize::<HashMap<String, HashMap<String, String>>>() {
        Ok(config) => config,
        Err(e) => {
            log::error!("Invalid format in {}: {}", config_path, e);
            exit(1);
        }
    };
    log::info!("Settings: {:?}", settings);

    let general = match settings.get("general") {
        Some(internal) => internal,
        None => {
            log::error!("Missing [internal] section in config.ini");
            exit(1);
        }
    };
    let mmsi = match general.get("mmsi").map(|v| v.parse::<u32>()) {
        None => {
            log::error!("Missing MMSI in config.ini");
            exit(1);
        }
        Some(Ok(interval)) => interval,
        Some(Err(e)) => {
            log::error!("Invalid MMSI in config.ini: {}", e);
            exit(1);
        }
    };
    let interval = match general.get("interval").map(|v| v.parse::<u64>()) {
        None => 60,
        Some(Ok(interval)) => interval,
        Some(Err(e)) => {
            log::error!("Invalid interval in config.ini: {}", e);
            exit(1);
        }
    };
    let location_interval = match general.get("location_interval").map(|v| v.parse::<u64>()) {
        None => 600,
        Some(Ok(interval)) => interval,
        Some(Err(e)) => {
            log::error!("Invalid location_interval in config.ini: {}", e);
            exit(1);
        }
    };
    let location_anchor_interval = match general
        .get("location_anchor_interval")
        .map(|v| v.parse::<u64>())
    {
        None => 86400,
        Some(Ok(interval)) => interval,
        Some(Err(e)) => {
            log::error!("Invalid location_anchor_interval in config.ini: {}", e);
            exit(1);
        }
    };

    let (tx, rx) = std::sync::mpsc::channel::<ParsedMessage>();
    let location = match settings.get("location") {
        Some(location) => location,
        None => {
            log::error!("Missing [location] section in config.ini");
            exit(1);
        }
    }
    .into_iter()
    .map(|(key, value)| {
        let address = value
            .parse::<NetworkEndpoint>()
            .map_err(|e| {
                log::error!("Invalid address '{}' in config.ini: {}", value, e);
                exit(1);
            })
            .unwrap();
        (key.clone(), address)
    })
    .collect();
    Builder::new()
        .name("location".to_string())
        .spawn(move || {
            location::work_thread(rx, location, mmsi, cli.cache_dir.as_str());
        })
        .unwrap();

    loop {
        let provider = match general
            .get("provider")
            .map(|v| v.parse::<NetworkEndpoint>())
        {
            None => {
                log::error!("Missing provider in config.ini");
                exit(1);
            }
            Some(Ok(provider)) => provider,
            Some(Err(e)) => {
                log::error!("Invalid interval in config.ini: {}", e);
                exit(1);
            }
        };

        let ais = match settings.get("ais") {
            Some(ais) => ais,
            None => {
                log::error!("Missing [ais] section in config.ini");
                exit(1);
            }
        };
        let ais = ais
            .into_iter()
            .map(|(key, value)| {
                let address = value
                    .parse::<NetworkEndpoint>()
                    .map_err(|e| {
                        log::error!("Invalid address '{}' in config.ini: {}", value, e);
                        exit(1);
                    })
                    .unwrap();
                (key.clone(), address)
            })
            .collect();

        let mut dispatcher = Dispatcher::new(
            provider,
            ais,
            tx.clone(),
            interval,
            location_interval,
            location_anchor_interval,
        );
        if let Err(e) = dispatcher.work() {
            log::error!("{}", e);
            std::thread::sleep(Duration::from_secs(1));
        }
    }
}

impl Dispatcher {
    fn new(
        provider: NetworkEndpoint,
        ais: HashMap<String, NetworkEndpoint>,
        location_tx: Sender<ParsedMessage>,
        interval: u64,
        location_interval: u64,
        location_anchor_interval: u64,
    ) -> Self {
        Dispatcher {
            provider,
            ais,
            location_tx,
            interval,
            location_interval,
            location_anchor_interval,
            nmea_parser: nmea_parser::NmeaParser::new(),
            last_sent: HashMap::new(),
            last_sent_location: Instant::now() - Duration::from_secs(location_interval),
        }
    }

    fn next_location_instant(&self, now: &Instant) -> Instant {
        let elapsed = now.duration_since(self.last_sent_location).as_secs();
        let wait_period = if elapsed < self.location_interval {
            self.location_interval - elapsed
        } else {
            0
        };
        *now + Duration::from_secs(wait_period)
    }
    fn next_location_anchor_instant(&self, now: &Instant) -> Instant {
        let elapsed = now.duration_since(self.last_sent_location).as_secs();
        let wait_period = if elapsed < self.location_anchor_interval {
            self.location_anchor_interval - elapsed
        } else {
            0
        };
        *now + Duration::from_secs(wait_period)
    }

    fn work(&mut self) -> io::Result<()> {
        let mut fragments = Vec::new();
        let mut allow_ais_for_location = true;
        let mut prev_lat = 0.0;
        let mut prev_long = 0.0;
        let now = Instant::now();
        let mut next_location_instant = self.next_location_instant(&now);
        let mut next_location_anchor_instant = self.next_location_anchor_instant(&now);

        loop {
            log::trace!("Waiting for message from provider");
            let message = read_from_provider(&mut self.provider)?;
            log::trace!("Received message: {}", message);

            for line in message.lines() {
                log::trace!("Received line: {}", line);
                match self.nmea_parser.parse_sentence(line) {
                    Ok(parsed_message) => {
                        if parsed_message == ParsedMessage::Incomplete {
                            fragments.push(line.to_string());
                            continue;
                        }
                        log::debug!("Parsed message: {:?}", parsed_message);
                        if let (Some(own_vessel), lat, long) = match &parsed_message {
                            ParsedMessage::VesselDynamicData(data) => (
                                Some(allow_ais_for_location && data.own_vessel),
                                data.latitude,
                                data.longitude,
                            ),
                            ParsedMessage::VesselStaticData(_data) => (Some(false), None, None),
                            ParsedMessage::Rmc(data) => {
                                allow_ais_for_location = false;
                                (Some(true), data.latitude, data.longitude)
                            }
                            _ => (None, None, None),
                        } {
                            fragments.push(line.to_string());
                            if self.check_last_sent(&parsed_message) {
                                self.broadcast_ais(&parsed_message, fragments.join("").as_bytes())?;
                            }
                            if own_vessel {
                                let now = Instant::now();
                                log::trace!(
                                    "Compare last sent location: {:?} interval {:?} anchor {:?}",
                                    now,
                                    next_location_instant,
                                    next_location_anchor_instant,
                                );
                                if now >= next_location_anchor_instant
                                    || (now >= next_location_instant
                                        && is_moving(lat, long, prev_lat, prev_long))
                                {
                                    prev_lat = lat.unwrap_or(0.0);
                                    prev_long = long.unwrap_or(0.0);
                                    self.last_sent_location = now;
                                    self.location_tx.send(parsed_message).unwrap();
                                    next_location_instant = self.next_location_instant(&now);
                                    next_location_anchor_instant =
                                        self.next_location_anchor_instant(&now);
                                }
                            }
                            fragments.clear();
                        }
                    }
                    Err(_e) => {
                        fragments.clear();
                    }
                }
            }
        }
    }

    fn broadcast_ais(&mut self, message: &ParsedMessage, nmea_message: &[u8]) -> io::Result<()> {
        log::debug!("Broadcasting message: {:?} / {:?}", message, nmea_message);
        for (key, address) in self.ais.iter_mut() {
            send_message(&nmea_message, key, address)?;
        }
        Ok(())
    }

    fn check_last_sent(&mut self, message: &ParsedMessage) -> bool {
        match message {
            ParsedMessage::VesselDynamicData(data) => {
                let interval = if data.own_vessel {
                    self.location_interval
                } else {
                    self.interval
                };
                let now = Instant::now();
                let elapsed = now - Duration::from_secs(interval);
                let last_sent = self.last_sent.entry(data.mmsi).or_insert(LastSent {
                    vessel_dynamic_data: elapsed,
                    vessel_static_data: elapsed,
                });
                if now.duration_since(last_sent.vessel_dynamic_data).as_secs() >= interval {
                    last_sent.vessel_dynamic_data = now;
                    return true;
                }
            }
            ParsedMessage::VesselStaticData(data) => {
                let interval = if data.own_vessel {
                    self.location_interval
                } else {
                    self.interval
                };
                let now = Instant::now();
                let elapsed = now - Duration::from_secs(interval);
                let last_sent = self.last_sent.entry(data.mmsi).or_insert(LastSent {
                    vessel_dynamic_data: elapsed,
                    vessel_static_data: elapsed,
                });
                if now.duration_since(last_sent.vessel_static_data).as_secs() >= interval {
                    last_sent.vessel_static_data = now;
                    return true;
                }
            }
            _ => {
                log::debug!("Ignoring message: {:?}", message);
            }
        }
        return false;
    }
}

fn is_moving(lat: Option<f64>, long: Option<f64>, prev_lat: f64, prev_long: f64) -> bool {
    if let (Some(lat), Some(long)) = (lat, long) {
        let lat_diff = (lat - prev_lat).abs();
        let long_diff = (long - prev_long).abs();
        if lat_diff > 0.001 || long_diff > 0.001 {
            return true;
        }
    }
    false
}

fn send_message(
    nmea_message: &[u8],
    key: &String,
    address: &mut NetworkEndpoint,
) -> io::Result<()> {
    match address.protocol {
        Protocol::TCP => {
            address.tcp_stream.retain(|writer| {
                if writer.peer_addr().is_err() {
                    log::warn!("Removing disconnected TCP stream");
                    false
                } else {
                    true
                }
            });

            if address.tcp_stream.len() == 0 {
                let stream = std::net::TcpStream::connect(address.addr).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!("{} ({}): {}", key, address.addr, e),
                    )
                })?;

                // Set the stream to use keepalive
                let sock_ref = socket2::SockRef::from(&stream);
                let mut ka = socket2::TcpKeepalive::new();
                ka = ka.with_time(Duration::from_secs(30));
                ka = ka.with_interval(Duration::from_secs(30));
                sock_ref.set_tcp_keepalive(&ka)?;

                log::info!("{}: Connected to {}", key, address);
                let writer = BufReaderDirectWriter::new(stream);
                address.tcp_stream.push(writer);
            }
            if let Some(tcp_stream) = address.tcp_stream.get_mut(0) {
                send_message_tcp(tcp_stream, nmea_message).map_err(|e| {
                    address.tcp_stream.clear();
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!("send_message tcp {} ({}): {}", key, address.addr, e),
                    )
                })?;
                log::debug!("{}: Sent message to {}", key, address);
            }
        }
        Protocol::UDP => {
            if address.udp_socket.is_none() {
                let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!("{} ({}): {}", key, address.addr, e),
                    )
                })?;
                UdpSocket::connect(&socket, address.addr)?;
                log::info!("{}: Connected to {}", key, address);
                address.udp_socket = Some(socket);
            }
            if let Some(udp_socket) = address.udp_socket.as_mut() {
                send_message_udp(udp_socket, nmea_message).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!("send_message udp {} ({}): {}", key, address.addr, e),
                    )
                })?;
            }
        }
        Protocol::TCPListen | Protocol::UDPListen => {}
    }
    Ok(())
}

fn read_from_provider(provider: &mut NetworkEndpoint) -> io::Result<String> {
    match provider.protocol {
        Protocol::TCP => {
            provider.tcp_stream.retain(|reader| {
                if reader.peer_addr().is_err() {
                    log::warn!("Removing disconnected TCP stream");
                    false
                } else {
                    true
                }
            });

            if provider.tcp_stream.len() == 0 {
                let stream = std::net::TcpStream::connect(provider.addr).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!("provider {}: {}", provider.addr, e),
                    )
                })?;
                log::info!("Connected to provider: {}", provider);
                let reader = BufReaderDirectWriter::new(stream);
                provider.tcp_stream.push(reader);
            }
            return read_message_tcp(&mut provider.tcp_stream[0]);
        }
        Protocol::TCPListen => {
            if provider.tcp_listener.is_none() {
                let listener = TcpListener::bind(provider.addr).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::AddrInUse,
                        format!("provider {}: {}", provider.addr, e),
                    )
                })?;
                listener.set_nonblocking(true)?;
                log::info!("Listening on: {}", provider);
                provider.tcp_listener = Some(listener);
            }
            if let Some(tcp_listener) = provider.tcp_listener.as_mut() {
                loop {
                    match tcp_listener.accept() {
                        Ok((stream, addr)) => {
                            log::info!("Accepted connection from: {}", addr);
                            stream.set_nonblocking(true)?;
                            let reader = BufReaderDirectWriter::new(stream);
                            provider.tcp_stream.push(reader);
                        }
                        Err(e) => {
                            if e.kind() == io::ErrorKind::WouldBlock {
                                // No connection available, continue
                                break;
                            }
                            log::error!("Error accepting connection: {}", e);
                            return Err(e);
                        }
                    }
                }
            }

            provider.tcp_stream.retain(|reader| {
                if reader.peer_addr().is_err() {
                    log::warn!("Removing disconnected TCP stream");
                    false
                } else {
                    true
                }
            });

            for reader in provider.tcp_stream.iter_mut() {
                if let Ok(message) = read_message_tcp(reader) {
                    if message.len() > 0 {
                        return Ok(message);
                    }
                }
            }
        }
        Protocol::UDP | Protocol::UDPListen => {
            if provider.udp_socket.is_none() {
                let socket = std::net::UdpSocket::bind(provider.addr)?;
                log::info!("Listening on: {}", provider);
                provider.udp_socket = Some(socket);
            }
            if let Some(udp_socket) = provider.udp_socket.as_mut() {
                return read_message_udp(udp_socket);
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::Other,
        "Failed to read from provider",
    ))
}

fn get_config_dir() -> PathBuf {
    let path = if path::Path::new("/etc/ais-forwarder").exists() {
        "/etc/ais-forwarder"
    } else if path::Path::new("/usr/local/etc/ais-forwarder").exists() {
        "/usr/local/etc/ais-forwarder"
    } else {
        log::error!(
            "No /etc/ais-forwarder or /usr/local/etc/ais-forwarder config directory found and no config file argument provided"
        );
        exit(1);
    };
    let path = path::Path::new(path);
    path.to_path_buf()
}
