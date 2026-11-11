This repository implements `stitch`, a tool for reliable `ssh` connections.

You can read a full design document for the tool in `doc/stitch.md`

The code in this repository is entirely owned by Claude; you are encouraged to refactor and improve it as you see fit. When you do, try to keep "refactoring" and "new functionality" or "bugfixes" as separate commits; your PRs may be reviewed by humans. In general, commit as you go; each logical unit of work should be committed, and unless you're stopping to ask for guidance on an implementation detail, most of the time you should have committed your work before returning to the user.

The **interface** and functionality of the tool are specified by the user; you should not substantially alter existing functionality or break backwards-compatibility without discussion with the user.

The project's Rust toolchain and other tools are pinned in `flake.nix` and brought in via `direnv`; Use `direnv exec` to get the right binaries on your PATH.
