//! Command-line definitions (`clap`), logging setup, and dispatch.

use std::ffi::OsString;
use std::os::fd::RawFd;
use std::path::Path;
use std::time::Duration;

use anyhow::bail;
use clap::{Args, Parser, Subcommand};

use crate::client::TunnelArgs;
use crate::handshake::Target;
use crate::{sshconfig, target};

#[derive(Debug, Parser)]
#[command(
    name = "stitch",
    version,
    about = "Reliable, resumable streams over ssh",
    long_about = "Reliable, resumable streams over ssh.\n\n\
        Logging is controlled by STITCH_LOG (e.g. STITCH_LOG=debug); the default is warn \
        (info for the server daemon's log)."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Connect stdin/stdout to [DEST:]PORT (or a command) on the far side of
    /// HOST, over a connection which survives network failures
    ///
    /// With only PORT, DEST is localhost. With neither, connects to localhost
    /// on the port of HOST's own sshd (from SSH_CONNECTION). Suitable for use
    /// as an ssh ProxyCommand.
    Tunnel(TunnelOpts),
    /// Run ssh to HOST over a stitch tunnel
    Ssh(SshOpts),
    /// Configure ssh so that `ssh HOST.stitch` runs over a stitch tunnel
    ///
    /// Writes ~/.ssh/stitch.conf and adds `Include stitch.conf` to the top of
    /// ~/.ssh/config if it is not already there.
    SshConfig(SshConfigOpts),
    /// Server-side entry point invoked by `stitch tunnel` over ssh
    #[command(name = "_server-shim", hide = true)]
    ServerShim,
    /// Per-session server daemon, spawned by the shim
    #[command(name = "_server", hide = true)]
    Server {
        /// Inherited fd of the listening rendezvous socket
        #[arg(long, value_name = "N")]
        listen_fd: RawFd,
    },
}

#[derive(Debug, Args)]
pub struct TunnelOpts {
    /// Host to ssh to
    pub host: String,

    /// Where to connect, from HOST: [DEST:]PORT (DEST defaults to localhost;
    /// write IPv6 addresses as [ADDR]:PORT)
    #[arg(value_name = "[DEST:]PORT", conflicts_with = "command")]
    pub destination: Option<String>,

    /// Instead of a TCP destination, run this command on HOST (after `--`)
    #[arg(last = true, value_name = "COMMAND")]
    pub command: Vec<String>,

    /// The ssh program to run
    #[arg(long, env = "STITCH_SSH", default_value = "ssh", value_name = "PROG")]
    pub ssh: OsString,

    /// Pass `-o OPT` to ssh (repeatable)
    #[arg(short = 'o', long = "ssh-option", value_name = "OPT")]
    pub ssh_options: Vec<String>,

    /// The stitch command to run on HOST, instead of `stitch` from the remote
    /// PATH. It is interpreted by the remote shell, so e.g. `~/bin/stitch`
    /// works.
    #[arg(
        long,
        env = "STITCH_REMOTE",
        default_value = "stitch",
        value_name = "CMD"
    )]
    pub remote_stitch: String,

    /// Seconds between heartbeats
    #[arg(long, value_name = "SECS", default_value = "10", value_parser = parse_secs)]
    pub heartbeat_interval: Duration,

    /// Seconds without traffic before reconnecting
    #[arg(long, value_name = "SECS", default_value = "20", value_parser = parse_secs)]
    pub timeout: Duration,

    /// Seconds to allow a reconnection attempt before abandoning it
    #[arg(long, value_name = "SECS", default_value = "30", value_parser = parse_secs)]
    pub connect_timeout: Duration,

    /// Remove SUFFIX from the end of HOST, if present (e.g. `.stitch`)
    #[arg(long, value_name = "SUFFIX")]
    pub strip_suffix: Option<String>,
}

#[derive(Debug, Args)]
pub struct SshOpts {
    /// The stitch command to run on HOST, instead of `stitch` from the remote
    /// PATH (passed on to `stitch tunnel --remote-stitch`)
    #[arg(long, value_name = "CMD")]
    pub remote_stitch: Option<String>,

    /// Host to connect to
    pub host: OsString,

    /// Further arguments, passed to ssh verbatim
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "SSH_ARGS"
    )]
    pub args: Vec<OsString>,
}

#[derive(Debug, Args)]
pub struct SshConfigOpts {
    /// Print the configuration snippet instead of installing it
    #[arg(long)]
    pub print: bool,
}

/// Parse a positive, finite number of seconds, possibly fractional.
fn parse_secs(s: &str) -> Result<Duration, String> {
    let secs: f64 = s
        .trim()
        .parse()
        .map_err(|_| format!("{s:?} is not a number of seconds"))?;
    if !secs.is_finite() || secs <= 0.0 {
        return Err(format!("{s:?} must be a positive number of seconds"));
    }
    Duration::try_from_secs_f64(secs).map_err(|e| format!("{s:?}: {e}"))
}

/// Remove `suffix` from the end of `host`, ignoring ASCII case (`ssh`
/// lowercases `%h`).
fn strip_host_suffix<'a>(host: &'a str, suffix: &str) -> &'a str {
    if host.len() >= suffix.len() {
        let split = host.len() - suffix.len();
        if host.is_char_boundary(split) && host[split..].eq_ignore_ascii_case(suffix) {
            return &host[..split];
        }
    }
    host
}

impl TunnelOpts {
    /// Validate and resolve into the configuration `client::run_tunnel` takes.
    pub fn resolve(self) -> anyhow::Result<TunnelArgs> {
        let host = match &self.strip_suffix {
            Some(suffix) => strip_host_suffix(&self.host, suffix),
            None => &self.host,
        };
        if host.is_empty() {
            bail!("empty host name {:?}", self.host);
        }
        let target = if !self.command.is_empty() {
            Target::Command {
                command: self.command.join(" "),
            }
        } else if let Some(dest) = &self.destination {
            let d = target::parse_destination(dest)?;
            Target::Tcp {
                host: d.host,
                port: Some(d.port),
            }
        } else {
            Target::Tcp {
                host: target::DEFAULT_HOST.to_owned(),
                port: None,
            }
        };
        Ok(TunnelArgs {
            host: host.to_owned(),
            target,
            ssh: self.ssh,
            ssh_options: self.ssh_options,
            remote_stitch: self.remote_stitch,
            heartbeat_interval: self.heartbeat_interval,
            timeout: self.timeout,
            connect_timeout: self.connect_timeout,
        })
    }
}

/// Parse a command line (including `argv[0]`).
///
/// This differs from `Cli::try_parse_from` for `stitch ssh`: everything after
/// HOST belongs to `ssh` and is passed through verbatim, so `clap` only sees
/// the command line up to and including HOST. Otherwise `clap` would swallow a
/// `--` which immediately follows HOST, to which `ssh` gives meaning
/// (`ssh HOST -- -x` runs the remote command `-x`), and would claim
/// `--remote-stitch` if it came first after HOST.
pub fn parse_cli<I, T>(args: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    if args.get(1).is_none_or(|a| a != "ssh") {
        return Cli::try_parse_from(&args);
    }
    // There are no options before the subcommand, and the only option after
    // it (other than `--help`) is `--remote-stitch`, so the command line is
    // `stitch ssh [--remote-stitch[=]CMD] [--] HOST ARGS...`.
    let mut host_at = 2;
    if let Some(a) = args.get(host_at) {
        if a == "--remote-stitch" {
            host_at += 2;
        } else if a.as_encoded_bytes().starts_with(b"--remote-stitch=") {
            host_at += 1;
        }
    }
    if args.get(host_at).is_some_and(|a| a == "--") {
        host_at += 1;
    }
    let split = (host_at + 1).min(args.len());
    let mut cli = Cli::try_parse_from(&args[..split])?;
    if let Command::Ssh(opts) = &mut cli.command {
        opts.args = args[split..].to_vec();
    }
    Ok(cli)
}

/// Set up logging to stderr, filtered by `STITCH_LOG` (default `warn`).
pub fn init_logging() {
    init_logging_with_default(log::LevelFilter::Warn);
}

/// Set up logging to stderr, filtered by `STITCH_LOG` (default `default`).
pub fn init_logging_with_default(default: log::LevelFilter) {
    let _ = env_logger::Builder::new()
        .filter_level(default)
        .parse_env(env_logger::Env::new().filter("STITCH_LOG"))
        .target(env_logger::Target::Stderr)
        .try_init();
}

fn ssh_config(opts: SshConfigOpts) -> anyhow::Result<i32> {
    // The configuration is long-lived, so refer to `stitch` by name and let
    // ssh find it on `$PATH`, rather than baking in the path of this binary
    // (which, under e.g. Nix, may be garbage-collected after an upgrade).
    let snippet = sshconfig::config_snippet(Path::new(sshconfig::STITCH_COMMAND))?;
    if sshconfig::find_in_path(sshconfig::STITCH_COMMAND).is_none() {
        eprintln!(
            "stitch: warning: `{}` was not found on $PATH; `ssh HOST{}` will not work until it is",
            sshconfig::STITCH_COMMAND,
            sshconfig::HOST_SUFFIX,
        );
    }
    if opts.print {
        print!("{snippet}");
        return Ok(0);
    }
    let home = sshconfig::home_dir()?;
    let out = sshconfig::install(&home, &snippet)?;
    if out.conf_written {
        println!("Wrote {}", out.conf_path.display());
    } else {
        println!("{} is up to date", out.conf_path.display());
    }
    match out.include {
        sshconfig::IncludeStatus::Added => println!(
            "Added `{}` to {}",
            sshconfig::INCLUDE_LINE,
            out.config_path.display()
        ),
        sshconfig::IncludeStatus::AlreadyPresent => {
            println!("{} already includes it", out.config_path.display())
        }
        sshconfig::IncludeStatus::PresentInBlock => eprintln!(
            "stitch: warning: {} includes {} only inside a Host or Match block, \
             where it may not take effect; consider moving `{}` to the top",
            out.config_path.display(),
            sshconfig::CONF_FILE_NAME,
            sshconfig::INCLUDE_LINE
        ),
    }
    Ok(0)
}

/// Run a parsed command line, returning the exit code.
pub fn run(cli: Cli) -> anyhow::Result<i32> {
    match cli.command {
        Command::Tunnel(opts) => crate::client::run_tunnel(opts.resolve()?),
        Command::Ssh(opts) => Err(sshconfig::exec_ssh(
            opts.remote_stitch.as_deref(),
            &opts.host,
            &opts.args,
        )),
        Command::SshConfig(opts) => ssh_config(opts),
        Command::ServerShim => crate::server::run_shim(),
        Command::Server { listen_fd } => crate::server::run_daemon(listen_fd),
    }
}

/// Entry point for the `stitch` binary; returns the process exit code.
pub fn main() -> i32 {
    let cli = parse_cli(std::env::args_os()).unwrap_or_else(|e| e.exit());
    // The session daemon logs to its own log file rather than a terminal, so
    // it records its (infrequent) opens, resumes and swaps by default.
    init_logging_with_default(match cli.command {
        Command::Server { .. } => log::LevelFilter::Info,
        _ => log::LevelFilter::Warn,
    });
    match run(cli) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("stitch: {e:#}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Command, clap::Error> {
        parse_cli(std::iter::once("stitch").chain(args.iter().copied())).map(|c| c.command)
    }

    fn tunnel(args: &[&str]) -> anyhow::Result<TunnelArgs> {
        match parse(args)? {
            Command::Tunnel(t) => t.resolve(),
            other => panic!("{other:?}"),
        }
    }

    fn tcp(host: &str, port: Option<u16>) -> Target {
        Target::Tcp {
            host: host.into(),
            port,
        }
    }

    #[test]
    fn verify_cli() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn tunnel_defaults() {
        // The defaults can be overridden from the environment; skip if so.
        if std::env::var_os("STITCH_SSH").is_some() || std::env::var_os("STITCH_REMOTE").is_some() {
            return;
        }
        let t = tunnel(&["tunnel", "host"]).unwrap();
        assert_eq!(
            t,
            TunnelArgs {
                host: "host".into(),
                target: tcp("localhost", None),
                ssh: "ssh".into(),
                ssh_options: vec![],
                remote_stitch: "stitch".into(),
                heartbeat_interval: Duration::from_secs(10),
                timeout: Duration::from_secs(20),
                connect_timeout: Duration::from_secs(30),
            }
        );
    }

    #[test]
    fn tunnel_destinations() {
        assert_eq!(
            tunnel(&["tunnel", "h", "22"]).unwrap().target,
            tcp("localhost", Some(22))
        );
        assert_eq!(
            tunnel(&["tunnel", "h", "127.0.0.1:2222"]).unwrap().target,
            tcp("127.0.0.1", Some(2222))
        );
        assert_eq!(
            tunnel(&["tunnel", "h", "[::1]:22"]).unwrap().target,
            tcp("::1", Some(22))
        );
        assert!(tunnel(&["tunnel", "h", "nope"]).is_err());
        assert!(parse(&["tunnel"]).is_err());
        assert!(parse(&["tunnel", "h", "22", "23"]).is_err());
    }

    #[test]
    fn tunnel_command() {
        let t = tunnel(&["tunnel", "h", "--", "echo", "-n", "hi there"]).unwrap();
        assert_eq!(
            t.target,
            Target::Command {
                command: "echo -n hi there".into()
            }
        );
        assert!(parse(&["tunnel", "h", "22", "--", "echo"]).is_err());
    }

    #[test]
    fn tunnel_options() {
        let t = tunnel(&[
            "tunnel",
            "--ssh",
            "/bin/fake-ssh",
            "-o",
            "Port=2222",
            "--ssh-option",
            "User=me",
            "--remote-stitch",
            "/opt/stitch",
            "--heartbeat-interval",
            "0.25",
            "--timeout=1.5",
            "--connect-timeout",
            "3",
            "--strip-suffix",
            ".stitch",
            "box.stitch",
            "80",
        ])
        .unwrap();
        assert_eq!(t.host, "box");
        assert_eq!(t.ssh, "/bin/fake-ssh");
        assert_eq!(t.ssh_options, ["Port=2222", "User=me"]);
        assert_eq!(t.remote_stitch, "/opt/stitch");
        assert_eq!(t.heartbeat_interval, Duration::from_millis(250));
        assert_eq!(t.timeout, Duration::from_millis(1500));
        assert_eq!(t.connect_timeout, Duration::from_secs(3));
        assert_eq!(t.target, tcp("localhost", Some(80)));

        for bad in ["0", "-1", "nan", "inf", "x", ""] {
            assert!(
                parse(&["tunnel", "h", "--timeout", bad]).is_err(),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn suffix_stripping() {
        let host = |args: &[&str]| tunnel(args).unwrap().host;
        assert_eq!(
            host(&["tunnel", "--strip-suffix", ".stitch", "a.b.stitch"]),
            "a.b"
        );
        assert_eq!(
            host(&["tunnel", "--strip-suffix", ".stitch", "A.STITCH"]),
            "A"
        );
        assert_eq!(
            host(&["tunnel", "--strip-suffix", ".stitch", "plain"]),
            "plain"
        );
        assert!(tunnel(&["tunnel", "--strip-suffix", ".stitch", ".stitch"]).is_err());
        assert_eq!(strip_host_suffix("é", "x"), "é");
        assert_eq!(strip_host_suffix("aé", "\u{e9}"), "a");
    }

    #[test]
    fn ssh_passthrough() {
        let Command::Ssh(s) = parse(&["ssh", "host", "-v", "--help", "ls", "-l"]).unwrap() else {
            panic!()
        };
        assert_eq!(s.host, "host");
        assert_eq!(s.args, ["-v", "--help", "ls", "-l"]);

        let Command::Ssh(s) = parse(&["ssh", "user@host"]).unwrap() else {
            panic!()
        };
        assert_eq!(s.host, "user@host");
        assert!(s.args.is_empty());

        let Command::Ssh(s) = parse(&["ssh", "host", "--", "x"]).unwrap() else {
            panic!()
        };
        assert_eq!(s.args, ["--", "x"]);
        let Command::Ssh(s) = parse(&["ssh", "host", "-v", "--", "x"]).unwrap() else {
            panic!()
        };
        assert_eq!(s.args, ["-v", "--", "x"]);
        let Command::Ssh(s) = parse(&["ssh", "host", "--", "--", "x"]).unwrap() else {
            panic!()
        };
        assert_eq!(s.args, ["--", "--", "x"]);
        let Command::Ssh(s) = parse(&["ssh", "host", "--"]).unwrap() else {
            panic!()
        };
        assert_eq!(s.args, ["--"]);
        assert!(parse(&["ssh"]).is_err());
        assert_eq!(s.remote_stitch, None);
    }

    #[test]
    fn ssh_remote_stitch() {
        for argv in [
            &["ssh", "--remote-stitch=/opt/stitch", "host", "--", "x"][..],
            &["ssh", "--remote-stitch", "/opt/stitch", "host", "--", "x"][..],
            &[
                "ssh",
                "--remote-stitch",
                "/opt/stitch",
                "--",
                "host",
                "--",
                "x",
            ][..],
        ] {
            let Command::Ssh(s) = parse(argv).unwrap() else {
                panic!()
            };
            assert_eq!(s.remote_stitch.as_deref(), Some("/opt/stitch"), "{argv:?}");
            assert_eq!(s.host, "host", "{argv:?}");
            assert_eq!(s.args, ["--", "x"], "{argv:?}");
        }
        // After HOST, it is an ssh argument like any other.
        let Command::Ssh(s) = parse(&["ssh", "host", "--remote-stitch=x"]).unwrap() else {
            panic!()
        };
        assert_eq!(s.remote_stitch, None);
        assert_eq!(s.args, ["--remote-stitch=x"]);
    }

    #[test]
    fn ssh_config_flags() {
        assert!(matches!(
            parse(&["ssh-config"]).unwrap(),
            Command::SshConfig(SshConfigOpts { print: false })
        ));
        assert!(matches!(
            parse(&["ssh-config", "--print"]).unwrap(),
            Command::SshConfig(SshConfigOpts { print: true })
        ));
    }

    #[test]
    fn hidden_commands() {
        assert!(matches!(
            parse(&["_server-shim"]).unwrap(),
            Command::ServerShim
        ));
        assert!(matches!(
            parse(&["_server", "--listen-fd", "3"]).unwrap(),
            Command::Server { listen_fd: 3 }
        ));
        assert!(parse(&["_server"]).is_err());
        use clap::CommandFactory;
        let help = Cli::command().render_help().to_string();
        assert!(!help.contains("_server"), "{help}");
    }
}
