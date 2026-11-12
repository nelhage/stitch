//! Parsing of the tunnel destination argument `[[<DEST>:]<PORT>]`, and of
//! `SSH_CONNECTION` (used on the server to find its own `sshd` port), and
//! validation of the host `stitch tunnel` runs `ssh` to.

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
    #[error("empty host name")]
    EmptySshHost,
    #[error("invalid host name {0:?}: it must not start with `-`")]
    SshHostIsOption(String),
    #[error("invalid host name {0:?}: it must not contain {1:?}")]
    BadSshHostChar(String, char),
}

/// Characters which may not appear in the host `stitch tunnel` runs `ssh` to:
/// those which a shell treats specially (quoting, expansion, globbing,
/// control operators, redirection, comments).
const SHELL_METACHARACTERS: &str = "*?[]{}~$`'\"\\;&|<>()!#^";

/// Check the host which `stitch tunnel` passes to `ssh` (`HOST`, or
/// `USER@HOST`; IPv6 literals are written without brackets, as `ssh` takes
/// them, possibly with a `%zone`).
///
/// The host usually arrives through an `ssh` `ProxyCommand`, which `ssh`
/// runs with the user's shell after substituting `%h` textually, and is then
/// handed to another `ssh` as an argument. Reject one which is empty, starts
/// with `-` (which `ssh` would take as an option), or contains whitespace,
/// control characters, or shell metacharacters; no real host name or address
/// needs any of those, and one which has them is at best a mistake.
///
/// This is a second line of defence only. It cannot make a `ProxyCommand`
/// which does not quote `%h` safe: there, the shell has already acted on the
/// host name before `stitch` sees any of it, and what a glob expands to may
/// arrive as an option rather than as the host.
pub fn validate_ssh_host(host: &str) -> Result<(), TargetError> {
    if host.is_empty() {
        return Err(TargetError::EmptySshHost);
    }
    if host.starts_with('-') {
        return Err(TargetError::SshHostIsOption(host.to_owned()));
    }
    match host
        .chars()
        .find(|&c| c.is_whitespace() || c.is_control() || SHELL_METACHARACTERS.contains(c))
    {
        Some(c) => Err(TargetError::BadSshHostChar(host.to_owned(), c)),
        None => Ok(()),
    }
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
    fn valid_ssh_hosts() {
        for s in [
            "host",
            "example.com",
            "Example.COM.",
            "user@example.com",
            "first.last+tag@host_1",
            "192.0.2.1",
            "::1",
            "me@fe80::1%eth0",
            "b\u{fc}cher.example",
            "a-b",
            "a=b,c:d/e",
        ] {
            assert_eq!(validate_ssh_host(s), Ok(()), "{s:?}");
        }
    }

    #[test]
    fn invalid_ssh_hosts() {
        assert_eq!(validate_ssh_host(""), Err(TargetError::EmptySshHost));
        for s in ["-", "-oProxyCommand=x", "--ssh-option=x", "-host"] {
            assert!(
                matches!(validate_ssh_host(s), Err(TargetError::SshHostIsOption(_))),
                "{s:?}"
            );
        }
        for s in [
            "*",
            "*.example.com",
            "ho?t",
            "h[ao]st",
            "[::1]",
            "{a,b}",
            "~",
            "~user",
            "$HOME",
            "`id`",
            "$(id)",
            "a b",
            " host",
            "host\t",
            "host\n",
            "a\rb",
            "a\0b",
            "a\u{1b}b",
            "a\u{a0}b",
            "a\u{2028}b",
            "it's",
            "\"host\"",
            "a\\b",
            "a;b",
            "a&b",
            "a|b",
            "a<b",
            "a>b",
            "a!b",
            "#host",
            "a^b",
            "user@-oProxyCommand=sh x;",
        ] {
            assert!(
                matches!(validate_ssh_host(s), Err(TargetError::BadSshHostChar(..))),
                "{s:?}"
            );
        }
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
