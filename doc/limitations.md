# Known limitations

`stitch` keeps a connection alive across network failures, but it runs inside
an environment it does not control: your `ssh` authentication setup, the
server's `sshd` configuration, and the server's session manager. This document
lists the cases where that environment prevents a session from surviving or
resuming, even though `stitch` itself is working as designed.

## Authentication must be non-interactive

`stitch` is only intended for use with `ssh` keys or another non-interactive
authentication mechanism: a key held by `ssh-agent`, an unencrypted key,
GSSAPI/Kerberos, or similar. Setups in which `ssh` has to ask the user
something on every connection are not supported. That includes password
authentication, an encrypted key that is not loaded into an agent, and
keyboard-interactive or per-connection two-factor prompts.

The reason is that every resume is a brand-new `ssh` login, made in the
background while your session is still on screen. Under `stitch ssh`, your
terminal belongs to the `ssh` session running through the tunnel, which holds
it in raw mode and reads everything you type. A prompt from the resuming `ssh`
has to share that terminal:

- The prompt text is written into the middle of whatever is on screen.
- Keystrokes are split unpredictably between the prompt and your session, so
  the login usually fails, and part of what you typed in answer to the prompt
  can be delivered to the remote shell as ordinary input.
- Each resume attempt, authentication included, is abandoned after
  `--connect-timeout` (30 seconds by default) and then retried, so the prompt
  comes back for as long as the connection is down.

The initial connection is different: it is made before the tunnel is in use
and has no timeout, so an interactive login works there. A session opened that
way can then never resume.

Related behaviour to be aware of even with non-interactive authentication:

- `stitch` retries a failed resume indefinitely, and cannot tell a temporary
  failure from a permanent one. If your key is revoked or the server's host key
  changes while you are disconnected, the session keeps retrying until you
  close it.
- The resuming `ssh` writes its own errors (for example "Could not resolve
  hostname") to your terminal on each failed attempt while you are offline.

## Server-side `ClientAliveInterval` ends idle-while-offline sessions

`stitch ssh` turns off `ssh`'s keepalives on the client (`ServerAliveInterval=0`
and `TCPKeepAlive=no`), so that the `ssh` session inside the tunnel does not
give up while the tunnel is down. It cannot turn off the *server's* keepalives.
Those are controlled by `ClientAliveInterval` and `ClientAliveCountMax` in the
server's `sshd_config`, and no client option overrides them.

When `ClientAliveInterval` is set, `sshd` periodically sends a probe through
the encrypted `ssh` session and expects an answer from the `ssh` client. That
client is on your machine, at the far end of the tunnel. While you are
disconnected, the `stitch` server can only buffer the probes; it cannot answer
them. After roughly `ClientAliveInterval` × `ClientAliveCountMax` seconds
without a reply, `sshd` closes the session and your remote shell is hung up.
When `stitch` next resumes, it does so successfully and then correctly reports
that the connection has closed.

For example, `ClientAliveInterval 300` with the default `ClientAliveCountMax 3`
limits a disconnection to about 15 minutes.

OpenSSH's default is `ClientAliveInterval 0` (disabled), so an unmodified
server is unaffected. Hardened configurations and some hosting images do set
it.

This can only be changed on the server, by its administrator. The session
inside the tunnel reaches `sshd` from the loopback address, so the keepalive
can be disabled for those connections alone, leaving it in force for everything
else:

```
Match Address 127.0.0.1,::1
    ClientAliveInterval 0
```

## `KillUserProcesses=yes` kills the server on the first disconnect

On a server using systemd-logind, the `stitch` server process is started from
your `ssh` login and belongs to that login's session scope. If
`KillUserProcesses=yes` is set in `logind.conf`, logind kills every process in
the scope when the login ends. The login ends when the connection carrying it
is lost, so the `stitch` server is killed, along with whatever it was connected
to, the first time your network drops. The next resume reports that the remote
session is gone.

This is the same restriction that applies to `tmux` and `screen`, and it
affects every kind of `stitch` target. Most distributions ship with
`KillUserProcesses=no`, in which case there is nothing to do. `root` is exempt
by default (`KillExcludeUsers`).

On a host where it is enabled, the `stitch` server has to be started outside
the login's session scope, with lingering enabled for your user (`loginctl
enable-linger`), in the same way as is needed to keep `tmux` alive on such a
host. `--remote-stitch` sets the command used to start `stitch` on the server
and can be used to wrap it accordingly.
