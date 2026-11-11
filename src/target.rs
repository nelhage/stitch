//! Parsing of the tunnel destination argument `[[<DEST>:]<PORT>]`, and of
//! `SSH_CONNECTION` (used on the server to find its own `sshd` port).

use std::fmt;

/// The host assumed when only a port is given.
pub const DEFAULT_HOST: &str = "localhost";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TargetError {
    #[error("invalid port {0:?}")]
    BadPort(String),
    #[error("empty host in destination {0:?}")]
    EmptyHost(String),
    #[error("invalid destination {0:?}: IPv6 addresses must be written in brackets, e.g. [::1]:22")]
    UnbracketedIpv6(String),
    #[error("invalid destination {0:?}")]
    Malformed(String),
    #[error("SSH_CONNECTION is not set")]
    NoSshConnection,
    #[error("cannot parse SSH_CONNECTION {0:?}")]
    BadSshConnection(String),
}

/// A parsed `[<DEST>:]<PORT>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destination {
    /// The destination host, without IPv6 brackets.
    pub host: String,
    pub port: u16,
}

impl fmt::Display for Destination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

fn parse_port(s: &str) -> Result<u16, TargetError> {
    // Reject signs and whitespace, which `u16::from_str` would partly accept.
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(TargetError::BadPort(s.to_owned()));
    }
    match s.parse::<u16>() {
        Ok(0) | Err(_) => Err(TargetError::BadPort(s.to_owned())),
        Ok(p) => Ok(p),
    }
}

/// Parse `[<DEST>:]<PORT>`.
///
/// - `22` → `localhost:22`
/// - `example.com:22`, `127.0.0.1:22`
/// - `[::1]:22` → host `::1` (brackets are required for IPv6 literals, since
///   otherwise the port would be ambiguous)
pub fn parse_destination(s: &str) -> Result<Destination, TargetError> {
    let (host, port) = if let Some(rest) = s.strip_prefix('[') {
        let (host, after) = rest
            .split_once(']')
            .ok_or_else(|| TargetError::Malformed(s.to_owned()))?;
        let port = after
            .strip_prefix(':')
            .ok_or_else(|| TargetError::Malformed(s.to_owned()))?;
        if host.is_empty() {
            return Err(TargetError::EmptyHost(s.to_owned()));
        }
        (host, port)
    } else {
        match s.rsplit_once(':') {
            None => (DEFAULT_HOST, s),
            Some((host, _)) if host.contains(':') => {
                return Err(TargetError::UnbracketedIpv6(s.to_owned()));
            }
            Some(("", _)) => return Err(TargetError::EmptyHost(s.to_owned())),
            Some((host, port)) => (host, port),
        }
    };
    if host.contains(['[', ']']) {
        return Err(TargetError::Malformed(s.to_owned()));
    }
    Ok(Destination {
        host: host.to_owned(),
        port: parse_port(port)?,
    })
}

/// Extract the server port (the fourth field) from an `SSH_CONNECTION` value
/// of the form `client_ip client_port server_ip server_port`.
pub fn parse_ssh_connection(s: &str) -> Result<u16, TargetError> {
    let fields: Vec<&str> = s.split_ascii_whitespace().collect();
    match fields[..] {
        [_, _, _, port] => {
            parse_port(port).map_err(|_| TargetError::BadSshConnection(s.to_owned()))
        }
        _ => Err(TargetError::BadSshConnection(s.to_owned())),
    }
}

/// The server's `sshd` port, from the `SSH_CONNECTION` environment variable.
pub fn ssh_connection_port() -> Result<u16, TargetError> {
    let v = std::env::var("SSH_CONNECTION").map_err(|_| TargetError::NoSshConnection)?;
    parse_ssh_connection(&v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dest(host: &str, port: u16) -> Destination {
        Destination {
            host: host.into(),
            port,
        }
    }

    #[test]
    fn valid_destinations() {
        assert_eq!(parse_destination("22").unwrap(), dest("localhost", 22));
        assert_eq!(
            parse_destination("example.com:2222").unwrap(),
            dest("example.com", 2222)
        );
        assert_eq!(
            parse_destination("127.0.0.1:65535").unwrap(),
            dest("127.0.0.1", 65535)
        );
        assert_eq!(parse_destination("[::1]:22").unwrap(), dest("::1", 22));
        assert_eq!(
            parse_destination("[fe80::1%eth0]:80").unwrap(),
            dest("fe80::1%eth0", 80)
        );
        assert_eq!(
            parse_destination("[localhost]:80").unwrap(),
            dest("localhost", 80)
        );
    }

    #[test]
    fn invalid_destinations() {
        for s in [
            "",
            "host",
            "host:",
            ":22",
            "host:0",
            "host:65536",
            "host:+22",
            "host:-1",
            "host: 22",
            "::1:22",
            "fe80::1",
            "[::1]",
            "[::1]22",
            "[::1:22",
            "[]:22",
            "[a]b]:22",
            "a]:22",
            "0",
        ] {
            assert!(parse_destination(s).is_err(), "{s:?} should be rejected");
        }
        assert!(matches!(
            parse_destination("::1:22"),
            Err(TargetError::UnbracketedIpv6(_))
        ));
    }

    #[test]
    fn display_roundtrips() {
        for s in ["localhost:22", "[::1]:22", "a.b:1"] {
            assert_eq!(parse_destination(s).unwrap().to_string(), s);
        }
    }

    #[test]
    fn ssh_connection() {
        assert_eq!(
            parse_ssh_connection("192.0.2.1 50000 192.0.2.2 22").unwrap(),
            22
        );
        assert_eq!(parse_ssh_connection("::1 50000 ::1 2222\n").unwrap(), 2222);
        for s in [
            "",
            "a b c",
            "a b c d e",
            "a b c x",
            "a b c 0",
            "a b c 70000",
        ] {
            assert!(parse_ssh_connection(s).is_err(), "{s:?}");
        }
    }
}
