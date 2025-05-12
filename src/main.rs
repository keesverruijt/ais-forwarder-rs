use config::Config;
use env_logger::Env;
use nmea_parser::ParsedMessage;
use std::collections::HashMap;
use std::net::{ SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket };
use std::process::exit;
use std::time::{ Duration, Instant };
use std::io::{ self, BufRead, Write };

mod buffer;
use buffer::BufReaderDirectWriter;

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
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid protocol")),
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
            return Err(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid address format, should be protocol://address"
                )
            );
        }
        let protocol = parts[0]
            .parse::<Protocol>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
        let mut addr = parts[1]
            .to_socket_addrs()
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("{}: {}", parts[1], e)
                )
            })?;
        let addr = addr
            .next()
            .ok_or_else(|| {
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
    nmea_parser: nmea_parser::NmeaParser,
    last_sent: HashMap<u32, LastSent>,
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    log::info!("ais-forwarder starting up");
    let settings = match
        Config::builder()
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

    loop {
        let provider = match general.get("provider").map(|v| v.parse::<NetworkEndpoint>()) {
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

        let mut dispatcher = Dispatcher::new(provider, ais, location, interval);
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
        interval: u64
    ) -> Self {
        Dispatcher {
            provider,
            ais,
            location,
            interval,
            nmea_parser: nmea_parser::NmeaParser::new(),
            last_sent: HashMap::new(),
        }
    }

    fn work(&mut self) -> io::Result<()> {
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
                        // Send to location
                        if self.check_last_sent(&parsed_message) {
                            fragments.push(line.to_string());

                            self.broadcast_message(parsed_message, fragments.join(""))?;
                        }
                        fragments.clear();
                    }
                    Err(_e) => {
                        fragments.clear();
                    }
                }
            }
        }
    }

    fn broadcast_message(
        &mut self,
        message: ParsedMessage,
        nmea_message: String
    ) -> io::Result<()> {
        log::info!("Broadcasting message: {:?} / {:?}", message, nmea_message);
        let own_vessel = match message {
            ParsedMessage::VesselDynamicData(data) => data.own_vessel,
            ParsedMessage::VesselStaticData(data) => data.own_vessel,
            _ => false,
        };
        if own_vessel {
            for (key, address) in self.location.iter_mut() {
                send_message(&nmea_message, key, address)?;
            }
        }
        for (key, address) in self.ais.iter_mut() {
            send_message(&nmea_message, key, address)?;
        }
        Ok(())
    }

    fn check_last_sent(&mut self, message: &ParsedMessage) -> bool {
        let now = Instant::now();
        let elapsed = now - Duration::from_secs(self.interval);
        match message {
            ParsedMessage::VesselDynamicData(data) => {
                let last_sent = self.last_sent.entry(data.mmsi).or_insert(LastSent {
                    vessel_dynamic_data: elapsed,
                    vessel_static_data: elapsed,
                });
                if now.duration_since(last_sent.vessel_dynamic_data).as_secs() >= self.interval {
                    last_sent.vessel_dynamic_data = now;
                    return true;
                }
            }
            ParsedMessage::VesselStaticData(data) => {
                let last_sent = self.last_sent.entry(data.mmsi).or_insert(LastSent {
                    vessel_dynamic_data: elapsed,
                    vessel_static_data: elapsed,
                });
                if
                    now.duration_since(last_sent.vessel_static_data).as_secs() >=
                    (self.interval as u64)
                {
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
    nmea_message: &String,
    key: &String,
    address: &mut NetworkEndpoint
) -> io::Result<()> {
    match address.protocol {
        Protocol::TCP => {
            if address.tcp_stream.len() == 0 {
                let stream = std::net::TcpStream
                    ::connect(address.addr)
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            format!("{}: {}", address.addr, e)
                        )
                    })?;
                log::info!("{}: Connected to AIS receiver: {}", key, address);
                let reader = BufReaderDirectWriter::new(stream);
                address.tcp_stream.push(reader);
            }
            if let Some(tcp_stream) = address.tcp_stream.get_mut(0) {
                send_message_tcp(tcp_stream, nmea_message.as_bytes())?;
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
                send_message_udp(udp_socket, nmea_message.as_bytes())?;
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
                let stream = std::net::TcpStream
                    ::connect(provider.addr)
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            format!("{}: {}", provider.addr, e)
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
                        format!("{}: {}", provider.addr, e)
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
    Err(io::Error::new(io::ErrorKind::Other, "Failed to read from provider"))
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
    message: &[u8]
) -> std::io::Result<()> {
    stream.write_all(message)?;
    Ok(())
}

fn read_message_tcp(stream: &mut BufReaderDirectWriter<TcpStream>) -> io::Result<String> {
    let mut buffer = String::with_capacity(72);
    let bytes_read = stream.read_line(&mut buffer)?;
    buffer.truncate(bytes_read);
    Ok(buffer)
}
