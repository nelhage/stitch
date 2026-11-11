//! The length-delimited JSON setup handshake (see "Setup handshake" in
//! `doc/implementation.md`).
//!
//! Each message is an 8-digit hexadecimal length (lowercase on write,
//! case-insensitive on read) followed by exactly that many bytes of JSON.
//!
//! The readers in this module **never read past the end of a message**: they
//! read exactly the 8-byte prefix and then exactly the announced number of
//! bytes. This matters because the same stream switches to the binary frame
//! protocol immediately afterwards. Consequently they must not be handed a
//! buffering reader (e.g. `BufReader`) whose buffer would be discarded
//! afterwards.
//!
//! Reading is split into two steps, available separately: [`read_raw`] returns
//! the JSON bytes of one message, and [`parse`] decodes them. The server shim
//! uses the raw form to forward a message to the daemon without dropping
//! fields it does not itself understand. [`read_message`] does both.

use std::io::{self, Read, Write};
use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The protocol version carried by every message we send.
pub const VERSION: u32 = 1;

/// Length of the hexadecimal length prefix, in bytes.
pub const PREFIX_LEN: usize = 8;

/// Maximum size of a message's JSON body, in bytes.
pub const MAX_MESSAGE_LEN: usize = 1 << 20;

/// Errors from reading, writing, or decoding a handshake message.
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error("I/O error during handshake: {0}")]
    Io(#[from] io::Error),
    /// The stream ended cleanly before the first byte of a message.
    #[error("connection closed before a handshake message was received")]
    Eof,
    /// The stream ended part-way through a message.
    #[error("connection closed part-way through a handshake message")]
    Truncated,
    /// The length prefix was not 8 hexadecimal digits.
    #[error("invalid handshake length prefix {0:?}")]
    BadPrefix(String),
    /// The announced length exceeds [`MAX_MESSAGE_LEN`].
    #[error("handshake message too large ({0} bytes; maximum {MAX_MESSAGE_LEN})")]
    TooLarge(usize),
    /// The body was not a valid message.
    #[error("invalid handshake message: {0}")]
    Json(#[from] serde_json::Error),
}

/// What the server should connect a new session to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Target {
    /// A TCP connection, made from the server. `port: None` (`null` on the
    /// wire) means "the server's own `sshd` port, from `SSH_CONNECTION`".
    Tcp {
        host: String,
        #[serde(default)]
        port: Option<u16>,
    },
    /// A command, run on the server as `$SHELL -c <command>`.
    Command { command: String },
}

/// Client → server: start a new session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Open {
    pub version: u32,
    /// How often the server should send heartbeats, in milliseconds.
    pub heartbeat_ms: u64,
    pub target: Target,
}

/// Client → server: move an existing session onto this channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resume {
    pub version: u32,
    pub session: String,
    /// The rendezvous socket, as returned in the server's [`OkReply`].
    pub socket: PathBuf,
    /// Strictly increasing per resume attempt; the initial open is epoch 0.
    pub epoch: u64,
}

/// Server → client: success. The channel switches to the frame protocol
/// immediately after this message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OkReply {
    pub version: u32,
    pub session: String,
    pub socket: PathBuf,
}

/// Server → client: failure. The channel is closed after this message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorReply {
    pub version: u32,
    pub code: ErrorCode,
    #[serde(default)]
    pub message: String,
}

/// The `code` of an [`ErrorReply`]. Codes we do not recognise (e.g. from a
/// newer peer) are preserved as [`ErrorCode::Unknown`] rather than failing to
/// parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    /// The rendezvous socket is missing or refusing connections.
    NoSession,
    /// A resume's epoch was not greater than the highest accepted.
    StaleEpoch,
    /// The request was malformed or of an unexpected type.
    BadRequest,
    /// The server could not connect to the target.
    TargetFailed,
    /// Anything else that went wrong on the server.
    Internal,
    /// A code this version does not know.
    Unknown(String),
}

impl ErrorCode {
    pub fn as_str(&self) -> &str {
        match self {
            ErrorCode::NoSession => "no_session",
            ErrorCode::StaleEpoch => "stale_epoch",
            ErrorCode::BadRequest => "bad_request",
            ErrorCode::TargetFailed => "target_failed",
            ErrorCode::Internal => "internal",
            ErrorCode::Unknown(s) => s,
        }
    }

    fn from_string(s: String) -> Self {
        match s.as_str() {
            "no_session" => ErrorCode::NoSession,
            "stale_epoch" => ErrorCode::StaleEpoch,
            "bad_request" => ErrorCode::BadRequest,
            "target_failed" => ErrorCode::TargetFailed,
            "internal" => ErrorCode::Internal,
            _ => ErrorCode::Unknown(s),
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for ErrorCode {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d).map(ErrorCode::from_string)
    }
}

/// A handshake message.
///
/// Unknown fields are ignored on decode. The `version` field is required but
/// not checked here; callers decide how to treat other versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Open(Open),
    Resume(Resume),
    Ok(OkReply),
    Error(ErrorReply),
}

impl Message {
    pub fn open(heartbeat_ms: u64, target: Target) -> Self {
        Message::Open(Open {
            version: VERSION,
            heartbeat_ms,
            target,
        })
    }

    pub fn resume(session: impl Into<String>, socket: impl Into<PathBuf>, epoch: u64) -> Self {
        Message::Resume(Resume {
            version: VERSION,
            session: session.into(),
            socket: socket.into(),
            epoch,
        })
    }

    pub fn ok(session: impl Into<String>, socket: impl Into<PathBuf>) -> Self {
        Message::Ok(OkReply {
            version: VERSION,
            session: session.into(),
            socket: socket.into(),
        })
    }

    pub fn error(code: ErrorCode, message: impl Into<String>) -> Self {
        Message::Error(ErrorReply {
            version: VERSION,
            code,
            message: message.into(),
        })
    }

    /// The `version` field of the message.
    pub fn version(&self) -> u32 {
        match self {
            Message::Open(m) => m.version,
            Message::Resume(m) => m.version,
            Message::Ok(m) => m.version,
            Message::Error(m) => m.version,
        }
    }

    /// The message's JSON body, without the length prefix.
    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("handshake messages always serialize")
    }

    /// The complete wire encoding: length prefix followed by JSON.
    pub fn encode(&self) -> Vec<u8> {
        frame(&self.to_json()).expect("handshake messages we construct are small")
    }
}

/// Decode a message from its JSON body (without the length prefix).
pub fn parse(json: &[u8]) -> Result<Message, HandshakeError> {
    Ok(serde_json::from_slice(json)?)
}

/// Prefix raw JSON bytes with their length, producing a complete wire
/// message. Fails if `json` exceeds [`MAX_MESSAGE_LEN`].
pub fn frame(json: &[u8]) -> Result<Vec<u8>, HandshakeError> {
    if json.len() > MAX_MESSAGE_LEN {
        return Err(HandshakeError::TooLarge(json.len()));
    }
    let mut out = Vec::with_capacity(PREFIX_LEN + json.len());
    out.extend_from_slice(format!("{:08x}", json.len()).as_bytes());
    out.extend_from_slice(json);
    Ok(out)
}

/// Decode a length prefix, checking it against [`MAX_MESSAGE_LEN`].
pub fn parse_prefix(prefix: &[u8; PREFIX_LEN]) -> Result<usize, HandshakeError> {
    // `from_str_radix` alone would accept a leading `+`.
    if !prefix.iter().all(u8::is_ascii_hexdigit) {
        return Err(HandshakeError::BadPrefix(
            String::from_utf8_lossy(prefix).into_owned(),
        ));
    }
    let s = std::str::from_utf8(prefix).expect("hex digits are ASCII");
    let len = u32::from_str_radix(s, 16).expect("validated hex") as usize;
    if len > MAX_MESSAGE_LEN {
        return Err(HandshakeError::TooLarge(len));
    }
    Ok(len)
}

/// Like `read_exact`, but distinguishes EOF before the first byte (`Ok(false)`)
/// from EOF part-way through (`Err(Truncated)`).
fn read_full<R: Read + ?Sized>(r: &mut R, buf: &mut [u8]) -> Result<bool, HandshakeError> {
    let mut done = 0;
    while done < buf.len() {
        match r.read(&mut buf[done..]) {
            Ok(0) if done == 0 => return Ok(false),
            Ok(0) => return Err(HandshakeError::Truncated),
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(true)
}

/// Read one message's JSON body from a blocking reader, consuming exactly the
/// message and nothing more.
pub fn read_raw<R: Read + ?Sized>(r: &mut R) -> Result<Vec<u8>, HandshakeError> {
    let mut prefix = [0u8; PREFIX_LEN];
    if !read_full(r, &mut prefix)? {
        return Err(HandshakeError::Eof);
    }
    let len = parse_prefix(&prefix)?;
    let mut body = vec![0u8; len];
    if !read_full(r, &mut body)? {
        return Err(HandshakeError::Truncated);
    }
    Ok(body)
}

/// Read and decode one message from a blocking reader, consuming exactly the
/// message and nothing more. Returns the raw JSON body alongside the decoded
/// message.
pub fn read_message<R: Read + ?Sized>(r: &mut R) -> Result<(Vec<u8>, Message), HandshakeError> {
    let raw = read_raw(r)?;
    let msg = parse(&raw)?;
    Ok((raw, msg))
}

/// Write one message to a blocking writer, and flush it.
pub fn write_message<W: Write + ?Sized>(w: &mut W, msg: &Message) -> io::Result<()> {
    w.write_all(&msg.encode())?;
    w.flush()
}

async fn read_full_async<R: AsyncRead + Unpin + ?Sized>(
    r: &mut R,
    buf: &mut [u8],
) -> Result<bool, HandshakeError> {
    let mut done = 0;
    while done < buf.len() {
        match r.read(&mut buf[done..]).await {
            Ok(0) if done == 0 => return Ok(false),
            Ok(0) => return Err(HandshakeError::Truncated),
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(true)
}

/// Async [`read_raw`]. Consumes exactly the message and nothing more (given an
/// unbuffered reader).
pub async fn read_raw_async<R: AsyncRead + Unpin + ?Sized>(
    r: &mut R,
) -> Result<Vec<u8>, HandshakeError> {
    let mut prefix = [0u8; PREFIX_LEN];
    if !read_full_async(r, &mut prefix).await? {
        return Err(HandshakeError::Eof);
    }
    let len = parse_prefix(&prefix)?;
    let mut body = vec![0u8; len];
    if !read_full_async(r, &mut body).await? {
        return Err(HandshakeError::Truncated);
    }
    Ok(body)
}

/// Async [`read_message`].
pub async fn read_message_async<R: AsyncRead + Unpin + ?Sized>(
    r: &mut R,
) -> Result<(Vec<u8>, Message), HandshakeError> {
    let raw = read_raw_async(r).await?;
    let msg = parse(&raw)?;
    Ok((raw, msg))
}

/// Async [`write_message`].
pub async fn write_message_async<W: AsyncWrite + Unpin + ?Sized>(
    w: &mut W,
    msg: &Message,
) -> io::Result<()> {
    w.write_all(&msg.encode()).await?;
    w.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn all_messages() -> Vec<Message> {
        vec![
            Message::open(
                10_000,
                Target::Tcp {
                    host: "localhost".into(),
                    port: Some(22),
                },
            ),
            Message::open(
                500,
                Target::Tcp {
                    host: "::1".into(),
                    port: None,
                },
            ),
            Message::open(
                10_000,
                Target::Command {
                    command: "echo hi".into(),
                },
            ),
            Message::resume("00ff", "/run/user/1000/stitch/00ff.sock", 3),
            Message::ok("00ff", "/run/user/1000/stitch/00ff.sock"),
            Message::error(ErrorCode::NoSession, "gone"),
            Message::error(ErrorCode::StaleEpoch, ""),
            Message::error(ErrorCode::BadRequest, "x"),
            Message::error(ErrorCode::TargetFailed, "x"),
            Message::error(ErrorCode::Internal, "x"),
            Message::error(ErrorCode::Unknown("from_the_future".into()), "x"),
        ]
    }

    #[test]
    fn roundtrip_blocking() {
        for m in all_messages() {
            let bytes = m.encode();
            assert_eq!(&bytes[..8], format!("{:08x}", bytes.len() - 8).as_bytes());
            let (raw, back) = read_message(&mut Cursor::new(&bytes)).unwrap();
            assert_eq!(back, m);
            assert_eq!(raw, &bytes[8..]);
        }
    }

    #[test]
    fn wire_format_matches_doc() {
        let json = |m: &Message| serde_json::from_slice::<serde_json::Value>(&m.to_json()).unwrap();
        assert_eq!(
            json(&Message::open(
                10_000,
                Target::Tcp {
                    host: "localhost".into(),
                    port: None
                }
            )),
            serde_json::json!({"type":"open","version":1,"heartbeat_ms":10000,
                "target":{"kind":"tcp","host":"localhost","port":null}})
        );
        assert_eq!(
            json(&Message::open(
                10_000,
                Target::Command {
                    command: "echo hi".into()
                }
            )),
            serde_json::json!({"type":"open","version":1,"heartbeat_ms":10000,
                "target":{"kind":"command","command":"echo hi"}})
        );
        assert_eq!(
            json(&Message::resume("ab", "/s/ab.sock", 3)),
            serde_json::json!({"type":"resume","version":1,"session":"ab",
                "socket":"/s/ab.sock","epoch":3})
        );
        assert_eq!(
            json(&Message::ok("ab", "/s/ab.sock")),
            serde_json::json!({"type":"ok","version":1,"session":"ab","socket":"/s/ab.sock"})
        );
        assert_eq!(
            json(&Message::error(ErrorCode::StaleEpoch, "old")),
            serde_json::json!({"type":"error","version":1,"code":"stale_epoch","message":"old"})
        );
    }

    #[test]
    fn does_not_over_read() {
        let mut bytes = Message::ok("ab", "/x").encode();
        bytes.extend_from_slice(b"FRAMEDATA");
        let mut cur = Cursor::new(&bytes);
        read_message(&mut cur).unwrap();
        let mut rest = Vec::new();
        Read::read_to_end(&mut cur, &mut rest).unwrap();
        assert_eq!(rest, b"FRAMEDATA");
    }

    /// A reader which returns one byte per call, to exercise short reads.
    struct Trickle<'a>(&'a [u8]);
    impl Read for Trickle<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.0.is_empty() || buf.is_empty() {
                return Ok(0);
            }
            buf[0] = self.0[0];
            self.0 = &self.0[1..];
            Ok(1)
        }
    }

    #[test]
    fn short_reads() {
        let m = Message::resume("ab", "/x", 7);
        let bytes = m.encode();
        assert_eq!(read_message(&mut Trickle(&bytes)).unwrap().1, m);
    }

    #[test]
    fn eof_and_truncation() {
        assert!(matches!(
            read_raw(&mut Cursor::new(b"")),
            Err(HandshakeError::Eof)
        ));
        assert!(matches!(
            read_raw(&mut Cursor::new(b"0000")),
            Err(HandshakeError::Truncated)
        ));
        let bytes = Message::ok("ab", "/x").encode();
        for cut in 1..bytes.len() {
            assert!(
                matches!(
                    read_raw(&mut Cursor::new(&bytes[..cut])),
                    Err(HandshakeError::Truncated)
                ),
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn empty_body_is_read_but_fails_to_parse() {
        assert_eq!(read_raw(&mut Cursor::new(b"00000000")).unwrap(), b"");
        assert!(matches!(
            read_message(&mut Cursor::new(b"00000000")),
            Err(HandshakeError::Json(_))
        ));
    }

    #[test]
    fn prefix_case_insensitive() {
        let mut json =
            br#"{"type":"error","version":1,"code":"internal","message":"0123456789"}"#.to_vec();
        // Pad with trailing whitespace so that the length is 0x5A.
        json.resize(0x5a, b' ');
        let len = format!("{:08X}", json.len());
        assert_eq!(len, "0000005A");
        let mut bytes = len.into_bytes();
        bytes.extend_from_slice(&json);
        let (_, m) = read_message(&mut Cursor::new(&bytes)).unwrap();
        assert_eq!(m, Message::error(ErrorCode::Internal, "0123456789"));
    }

    #[test]
    fn bad_prefix() {
        for p in [
            &b"0000004g"[..],
            b"+0000040",
            b" 0000040",
            b"0x000040",
            b"-0000001",
        ] {
            let mut bytes = p.to_vec();
            bytes.extend_from_slice(&[b'x'; 0x40]);
            assert!(
                matches!(
                    read_raw(&mut Cursor::new(&bytes)),
                    Err(HandshakeError::BadPrefix(_))
                ),
                "{:?}",
                String::from_utf8_lossy(p)
            );
        }
    }

    #[test]
    fn oversize() {
        let max = format!("{MAX_MESSAGE_LEN:08x}");
        assert_eq!(
            parse_prefix(max.as_bytes().try_into().unwrap()).unwrap(),
            MAX_MESSAGE_LEN
        );
        let over = format!("{:08x}", MAX_MESSAGE_LEN + 1);
        assert!(matches!(
            read_raw(&mut Cursor::new(over.as_bytes())),
            Err(HandshakeError::TooLarge(_))
        ));
        assert!(matches!(
            read_raw(&mut Cursor::new(b"ffffffff")),
            Err(HandshakeError::TooLarge(_))
        ));
        assert!(matches!(
            frame(&vec![b' '; MAX_MESSAGE_LEN + 1]),
            Err(HandshakeError::TooLarge(_))
        ));
    }

    #[test]
    fn unknown_fields_ignored() {
        let json = br#"{"type":"open","version":1,"heartbeat_ms":5,"future":{"a":1},
            "target":{"kind":"tcp","host":"h","port":80,"extra":true}}"#;
        assert_eq!(
            parse(json).unwrap(),
            Message::open(
                5,
                Target::Tcp {
                    host: "h".into(),
                    port: Some(80)
                }
            )
        );
        let json = br#"{"type":"error","version":2,"code":"exploded","message":"m","x":1}"#;
        let m = parse(json).unwrap();
        assert_eq!(m.version(), 2);
        assert_eq!(
            m,
            Message::Error(ErrorReply {
                version: 2,
                code: ErrorCode::Unknown("exploded".into()),
                message: "m".into()
            })
        );
    }

    #[test]
    fn missing_port_is_none() {
        let json =
            br#"{"type":"open","version":1,"heartbeat_ms":5,"target":{"kind":"tcp","host":"h"}}"#;
        let Message::Open(o) = parse(json).unwrap() else {
            panic!()
        };
        assert_eq!(
            o.target,
            Target::Tcp {
                host: "h".into(),
                port: None
            }
        );
    }

    #[test]
    fn invalid_messages() {
        for json in [
            &br#"{"type":"nope","version":1}"#[..],
            br#"{"version":1}"#,
            br#"{"type":"resume","version":1,"session":"a","socket":"/x"}"#,
            br#"{"type":"open","heartbeat_ms":5,"target":{"kind":"command","command":"x"}}"#,
            br#"{"type":"open","version":1,"heartbeat_ms":5,"target":{"kind":"udp"}}"#,
            br#"[]"#,
            b"not json",
        ] {
            assert!(
                matches!(parse(json), Err(HandshakeError::Json(_))),
                "{}",
                String::from_utf8_lossy(json)
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn roundtrip_async_no_over_read() {
        let (mut a, mut b) = tokio::io::duplex(64);
        let msgs = all_messages();
        let sent = msgs.clone();
        let writer = tokio::spawn(async move {
            for m in &sent {
                write_message_async(&mut a, m).await.unwrap();
            }
            a.write_all(b"TAIL").await.unwrap();
        });
        for m in &msgs {
            let (raw, got) = read_message_async(&mut b).await.unwrap();
            assert_eq!(&got, m);
            assert_eq!(raw, m.to_json());
        }
        let mut tail = [0u8; 4];
        b.read_exact(&mut tail).await.unwrap();
        assert_eq!(&tail, b"TAIL");
        writer.await.unwrap();
        assert!(matches!(
            read_raw_async(&mut b).await,
            Err(HandshakeError::Eof)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_truncated_and_bad() {
        let mut r: &[u8] = b"00000002{";
        assert!(matches!(
            read_raw_async(&mut r).await,
            Err(HandshakeError::Truncated)
        ));
        let mut r: &[u8] = b"zzzzzzzz";
        assert!(matches!(
            read_raw_async(&mut r).await,
            Err(HandshakeError::BadPrefix(_))
        ));
    }

    #[test]
    fn blocking_write() {
        let mut out = Vec::new();
        let m = Message::ok("a", "/b");
        write_message(&mut out, &m).unwrap();
        assert_eq!(out, m.encode());
    }
}
