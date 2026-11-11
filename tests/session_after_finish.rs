//! The server's "linger" (see "Server side" in `doc/implementation.md`)
//! relies on a finished [`Session`] still serving a new channel: if the
//! peer did not receive our final acknowledgement and resumes, the `FIRST`
//! frame on the new channel must carry it. These tests check that through
//! the public `Session` API.

use std::time::Instant;

use stitch::core::{Config, Session};
use stitch::frame::{FrameDecoder, FrameHeader};

/// Takes all pending output from `s` as raw bytes.
fn take_tx(s: &mut Session, now: Instant) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let d = s.transmit_data(now).to_vec();
        if d.is_empty() {
            return out;
        }
        s.consume_transmit(d.len(), now);
        out.extend(d);
    }
}

fn headers(bytes: &[u8]) -> Vec<FrameHeader> {
    let mut d = FrameDecoder::new();
    d.push(bytes);
    let mut out = Vec::new();
    while let Some((h, _)) = d.next_frame().unwrap() {
        out.push(h);
    }
    out
}

/// Exchanges output between `a` and `b` until both are quiet.
fn flush(a: &mut Session, b: &mut Session, now: Instant) {
    loop {
        let ab = take_tx(a, now);
        let ba = take_tx(b, now);
        if ab.is_empty() && ba.is_empty() {
            return;
        }
        b.receive(&ab, now).unwrap();
        a.receive(&ba, now).unwrap();
    }
}

#[test]
fn finished_session_serves_a_new_channel() {
    let now = Instant::now();
    // `server` shuts down first; `client` shuts down later, and loses the
    // server's acknowledgement of that.
    let mut server = Session::new(Config::default());
    let mut client = Session::new(Config::default());
    server.swap_channel(now);
    client.swap_channel(now);
    flush(&mut server, &mut client, now);

    server.close_app_send();
    flush(&mut server, &mut client, now);
    assert!(client.app_eof());

    client.close_app_send();
    let shutdown = take_tx(&mut client, now);
    server.receive(&shutdown, now).unwrap();
    // The server's final acknowledgement is lost with the channel.
    let lost = take_tx(&mut server, now);
    assert!(!lost.is_empty());
    assert!(server.is_finished());
    assert!(!client.is_finished());
    assert!(!client.shutdown_acked());

    // A resume: both ends swap. The finished server still sends a FIRST
    // frame carrying its acknowledgement of the client's shutdown.
    server.swap_channel(now);
    client.swap_channel(now);
    assert!(!server.is_finished(), "not finished until the FIRST frame");
    let first = take_tx(&mut server, now);
    let h = headers(&first);
    assert!(!h.is_empty());
    assert!(h[0].first);
    assert_eq!(h[0].ack, client.send_data_end() + 1);
    assert!(server.is_finished());

    // The client's rewound shutdown is a harmless duplicate to the server.
    let from_client = take_tx(&mut client, now);
    assert!(headers(&from_client)[0].first);
    server.receive(&from_client, now).unwrap();
    client.receive(&first, now).unwrap();
    flush(&mut server, &mut client, now);
    assert!(client.is_finished());
    assert!(server.is_finished());
    assert!(server.error().is_none() && client.error().is_none());
}
