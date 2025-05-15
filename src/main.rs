use config::Config;
use env_logger::Env;
use nmea_parser::ParsedMessage;
use nmea_parser::ais::VesselDynamicData;
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::process::exit;
use std::time::{Duration, Instant};
use time::macros::format_description;
use time::{self, UtcDateTime};

mod buffer;
mod cache;

use buffer::BufReaderDirectWriter;
use cache::Persistence;

enum Protocol {
    TCP,
    UDP,
    TCPListen,
    UDPListen,
}
impl std::str::FromStr for Protocol {
    type Err = std::io::Error;
    fn from_str(s: &str) -> io::Result<Self> {
        match s {
            "tcp" => Ok(Protocol::TCP),
            "udp" => Ok(Protocol::UDP),
            "tcp-listen" => Ok(Protocol::TCPListen),
            "udp-listen" => Ok(Protocol::UDPListen),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid protocol",
            )),
        }
    }
}
impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Protocol::TCP => write!(f, "tcp"),
            Protocol::UDP => write!(f, "udp"),
            Protocol::TCPListen => write!(f, "tcp-listen"),
            Protocol::UDPListen => write!(f, "udp-listen"),
        }
    }
}
impl std::fmt::Debug for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Protocol::TCP => write!(f, "tcp"),
            Protocol::UDP => write!(f, "udp"),
            Protocol::TCPListen => write!(f, "tcp-listen"),
            Protocol::UDPListen => write!(f, "udp-listen"),
        }
    }
}

struct NetworkEndpoint {
    protocol: Protocol,
    addr: SocketAddr,
    tcp_listener: Option<std::net::TcpListener>,
    tcp_stream: Vec<BufReaderDirectWriter<std::net::TcpStream>>, // List of connected incoming TCP streams or single outgoing stream
    udp_socket: Option<std::net::UdpSocket>,
}

impl std::str::FromStr for NetworkEndpoint {
    type Err = std::io::Error;

    fn from_str(s: &str) -> std::io::Result<Self> {
        let parts = s.split("://").collect::<Vec<_>>();
        if parts.len() != 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid address format, should be protocol://address",
            ));
        }
        let protocol = parts[0]
            .parse::<Protocol>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
        let mut addr = parts[1].to_socket_addrs().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{}: {}", parts[1], e),
            )
        })?;
        let addr = addr.next().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "No address found")
        })?;
        Ok(NetworkEndpoint {
            protocol,
            addr,
            tcp_listener: None,
            tcp_stream: Vec::new(),
            udp_socket: None,
        })
    }
}
impl std::fmt::Display for NetworkEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}", self.protocol, self.addr)
    }
}
impl std::fmt::Debug for NetworkEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}", self.protocol, self.addr)
    }
}
impl std::convert::From<NetworkEndpoint> for SocketAddr {
    fn from(addr: NetworkEndpoint) -> Self {
        addr.addr
    }
}

struct LastSent {
    vessel_dynamic_data: Instant,
    vessel_static_data: Instant,
}

struct Dispatcher {
    provider: NetworkEndpoint,
    ais: HashMap<String, NetworkEndpoint>,
    location: HashMap<String, NetworkEndpoint>,
    interval: u64,
    location_interval: u64,
    nmea_parser: nmea_parser::NmeaParser,
    last_sent: HashMap<u32, LastSent>,
    last_sent_location: Instant,
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    log::info!("ais-forwarder starting up");
    let settings = match Config::builder()
        // Add in `./ais-forwarder.ini`
        .add_source(config::File::with_name("ais-forwarder.ini"))
        .build()
    {
        Ok(config) => config,
        Err(e) => {
            log::error!("Error loading ais-forwarder.ini: {}", e);
            exit(1);
        }
    };

    let settings = match settings.try_deserialize::<HashMap<String, HashMap<String, String>>>() {
        Ok(config) => config,
        Err(e) => {
            log::error!("Invalid format in ais-forwarder.ini: {}", e);
            exit(1);
        }
    };
    log::info!("Settings: {:?}", settings);

    let general = match settings.get("general") {
        Some(internal) => internal,
        None => {
            log::error!("Missing [internal] section in ais-forwarder.ini");
            exit(1);
        }
    };
    let interval = match general.get("interval").map(|v| v.parse::<u64>()) {
        None => 60,
        Some(Ok(interval)) => interval,
        Some(Err(e)) => {
            log::error!("Invalid interval in ais-forwarder.ini: {}", e);
            exit(1);
        }
    };
    let location_interval = match general.get("location_interval").map(|v| v.parse::<u64>()) {
        None => 600,
        Some(Ok(interval)) => interval,
        Some(Err(e)) => {
            log::error!("Invalid location_interval in ais-forwarder.ini: {}", e);
            exit(1);
        }
    };

    loop {
        let provider = match general
            .get("provider")
            .map(|v| v.parse::<NetworkEndpoint>())
        {
            None => {
                log::error!("Missing provider in ais-forwarder.ini");
                exit(1);
            }
            Some(Ok(provider)) => provider,
            Some(Err(e)) => {
                log::error!("Invalid interval in ais-forwarder.ini: {}", e);
                exit(1);
            }
        };

        let ais = match settings.get("ais") {
            Some(ais) => ais,
            None => {
                log::error!("Missing [ais] section in ais-forwarder.ini");
                exit(1);
            }
        };
        let ais = ais
            .into_iter()
            .map(|(key, value)| {
                let address = value
                    .parse::<NetworkEndpoint>()
                    .map_err(|e| {
                        log::error!("Invalid address '{}' in ais-forwarder.ini: {}", value, e);
                        exit(1);
                    })
                    .unwrap();
                (key.clone(), address)
            })
            .collect();

        let location = match settings.get("location") {
            Some(location) => location,
            None => {
                log::error!("Missing [location] section in ais-forwarder.ini");
                exit(1);
            }
        };
        let location = location
            .into_iter()
            .map(|(key, value)| {
                let address = value
                    .parse::<NetworkEndpoint>()
                    .map_err(|e| {
                        log::error!("Invalid address '{}' in ais-forwarder.ini: {}", value, e);
                        exit(1);
                    })
                    .unwrap();
                (key.clone(), address)
            })
            .collect();

        let mut dispatcher = Dispatcher::new(provider, ais, location, interval, location_interval);
        if let Err(e) = dispatcher.work() {
            log::error!("{}", e);
            std::thread::sleep(Duration::from_secs(10));
        }
    }
}

impl Dispatcher {
    fn new(
        provider: NetworkEndpoint,
        ais: HashMap<String, NetworkEndpoint>,
        location: HashMap<String, NetworkEndpoint>,
        interval: u64,
        location_interval: u64,
    ) -> Self {
        Dispatcher {
            provider,
            ais,
            location,
            interval,
            location_interval,
            nmea_parser: nmea_parser::NmeaParser::new(),
            last_sent: HashMap::new(),
            last_sent_location: Instant::now() - Duration::from_secs(location_interval),
        }
    }

    fn resend_messages(&mut self, persistence: &Persistence) -> io::Result<()> {
        for item in persistence.iter() {
            match item {
                Ok((key, value)) => {
                    let key = &key.to_vec();
                    let value = &value.to_vec();
                    let skey = String::from_utf8_lossy(&key);
                    let svalue = String::from_utf8_lossy(&value);
                    log::info!("Resending message: {}: {}", skey, svalue);
                    for (key, address) in self.location.iter_mut() {
                        send_message(value, key, address)?;
                    }
                    persistence.remove(key);
                    persistence.flush();
                }
                Err(e) => {
                    log::error!("Error reading from database: {}", e);
                }
            }
        }
        Ok(())
    }

    fn work(&mut self) -> io::Result<()> {
        let persistence = Persistence::new();

        self.resend_messages(&persistence)?;

        let mut fragments = Vec::new();
        loop {
            let message = read_from_provider(&mut self.provider)?;

            for line in message.lines() {
                match self.nmea_parser.parse_sentence(line) {
                    Ok(parsed_message) => {
                        if parsed_message == ParsedMessage::Incomplete {
                            fragments.push(line.to_string());
                            continue;
                        }
                        log::debug!("Parsed message: {:?}", parsed_message);
                        if let Some(own_vessel) = match &parsed_message {
                            ParsedMessage::VesselDynamicData(data) => {
                                if data.own_vessel {
                                    Some(Some(data))
                                } else {
                                    Some(None)
                                }
                            }
                            ParsedMessage::VesselStaticData(_data) => Some(None),
                            _ => None,
                        } {
                            fragments.push(line.to_string());
                            if let Some(dynamic_data) = own_vessel {
                                let now = Instant::now();
                                if now.duration_since(self.last_sent_location).as_secs()
                                    >= self.location_interval
                                {
                                    self.last_sent_location = now;
                                    self.broadcast_location(dynamic_data, &persistence)?;
                                }
                            }
                            if self.check_last_sent(&parsed_message) {
                                self.broadcast_ais(parsed_message, fragments.join("").as_bytes())?;
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

    fn broadcast_ais(&mut self, message: ParsedMessage, nmea_message: &[u8]) -> io::Result<()> {
        log::info!("Broadcasting message: {:?} / {:?}", message, nmea_message);
        for (key, address) in self.ais.iter_mut() {
            send_message(&nmea_message, key, address)?;
        }
        Ok(())
    }

    fn broadcast_location(
        &mut self,
        message: &VesselDynamicData,
        persistence: &Persistence,
    ) -> io::Result<()> {
        let now = UtcDateTime::now();
        const TIME_FORMAT: &[time::format_description::BorrowedFormatItem<'_>] =
            format_description!("[hour][minute][second]");
        const DATE_FORMAT: &[time::format_description::BorrowedFormatItem<'_>] =
            format_description!("[day][month][year repr:last_two]");

        let nmea_message = format!(
            "{}$GNRMC,{},A,{},{},{},{},{},,,A",
            message.mmsi,
            now.format(TIME_FORMAT).unwrap(),
            Self::format_lat_long(message.latitude, true),
            Self::format_lat_long(message.longitude, false),
            "", // Speed over ground,
            "", // Course over ground,
            now.format(DATE_FORMAT).unwrap(),
        );

        log::debug!("Broadcasting location message: {:?}", nmea_message);
        let nmea_message = nmea_message.as_bytes();
        for (key, address) in self.location.iter_mut() {
            if let Err(e) = send_message(&nmea_message, key, address) {
                let db_key = format!("{}-{}", now, key);
                log::error!("Error sending location message to {}: {}", key, e);
                persistence.store(db_key.as_bytes(), nmea_message);
                persistence.flush();
            }
        }
        Ok(())
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

fn send_message(
    nmea_message: &[u8],
    key: &String,
    address: &mut NetworkEndpoint,
) -> io::Result<()> {
    match address.protocol {
        Protocol::TCP => {
            if address.tcp_stream.len() == 0 {
                let stream = std::net::TcpStream::connect(address.addr).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!("{}: {}", address.addr, e),
                    )
                })?;
                log::info!("{}: Connected to AIS receiver: {}", key, address);
                let reader = BufReaderDirectWriter::new(stream);
                address.tcp_stream.push(reader);
            }
            if let Some(tcp_stream) = address.tcp_stream.get_mut(0) {
                send_message_tcp(tcp_stream, nmea_message)?;
            }
        }
        Protocol::UDP => {
            if address.udp_socket.is_none() {
                let socket = UdpSocket::bind("0.0.0.0:0")?;
                UdpSocket::connect(&socket, address.addr)?;
                log::info!("{}: Connected to AIS receiver: {}", key, address);
                address.udp_socket = Some(socket);
            }
            if let Some(udp_socket) = address.udp_socket.as_mut() {
                send_message_udp(udp_socket, nmea_message)?;
            }
        }
        Protocol::TCPListen | Protocol::UDPListen => {}
    }
    Ok(())
}

fn read_from_provider(provider: &mut NetworkEndpoint) -> io::Result<String> {
    match provider.protocol {
        Protocol::TCP => {
            if provider.tcp_stream.len() == 0 {
                let stream = std::net::TcpStream::connect(provider.addr).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!("{}: {}", provider.addr, e),
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
                        format!("{}: {}", provider.addr, e),
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
            for reader in provider.tcp_stream.iter_mut() {
                if let Ok(message) = read_message_tcp(reader) {
                    return Ok(message);
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

fn send_message_udp(stream: &mut std::net::UdpSocket, message: &[u8]) -> std::io::Result<()> {
    stream.send(message)?;
    Ok(())
}

fn read_message_udp(stream: &mut std::net::UdpSocket) -> std::io::Result<String> {
    let mut buffer = vec![0; 1024];
    let (bytes_read, _) = stream.recv_from(&mut buffer)?;
    buffer.truncate(bytes_read);
    let buffer = String::from_utf8_lossy(&buffer).to_string();
    Ok(buffer)
}

fn send_message_tcp(
    stream: &mut BufReaderDirectWriter<TcpStream>,
    message: &[u8],
) -> std::io::Result<()> {
    stream.write_all(message)?;
    stream.flush()?;
    Ok(())
}

fn read_message_tcp(stream: &mut BufReaderDirectWriter<TcpStream>) -> io::Result<String> {
    let mut buffer = String::with_capacity(72);
    let bytes_read = stream.read_line(&mut buffer)?;
    buffer.truncate(bytes_read);
    Ok(buffer)
}
