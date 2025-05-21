use std::io::{self, BufRead, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};

pub mod buffer;
use buffer::BufReaderDirectWriter;

pub enum Protocol {
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

pub struct NetworkEndpoint {
    pub protocol: Protocol,
    pub addr: SocketAddr,
    pub tcp_listener: Option<std::net::TcpListener>,
    pub tcp_stream: Vec<BufReaderDirectWriter<std::net::TcpStream>>, // List of connected incoming TCP streams or single outgoing stream
    pub udp_socket: Option<std::net::UdpSocket>,
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

pub fn send_message_udp(stream: &mut std::net::UdpSocket, message: &[u8]) -> std::io::Result<()> {
    stream.send(message)?;
    Ok(())
}

pub fn read_message_udp(stream: &mut std::net::UdpSocket) -> std::io::Result<String> {
    let mut buffer = vec![0; 1024];
    let (bytes_read, _) = stream.recv_from(&mut buffer)?;
    buffer.truncate(bytes_read);
    let buffer = String::from_utf8_lossy(&buffer).to_string();
    Ok(buffer)
}

pub fn send_message_tcp(
    stream: &mut BufReaderDirectWriter<TcpStream>,
    message: &[u8],
) -> std::io::Result<()> {
    stream.write_all(message)?;
    stream.flush()?;
    Ok(())
}

pub fn read_message_tcp(stream: &mut BufReaderDirectWriter<TcpStream>) -> io::Result<String> {
    let mut buffer = String::with_capacity(72);
    let bytes_read = stream.read_line(&mut buffer)?;
    buffer.truncate(bytes_read);
    Ok(buffer)
}
