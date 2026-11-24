#!/bin/sh
# Install stitch from a GitHub release.
#
#   curl -fsSL https://raw.githubusercontent.com/nelhage/stitch/main/scripts/install.sh | sh
#
# Options can be passed through the pipe with `sh -s --`:
#
#   curl -fsSL .../install.sh | sh -s -- --version v0.1 --dir ~/bin
#
# This downloads stitch-<target>.tar.gz for the current platform, and the
# release's SHA256SUMS, from the release built by .github/workflows/release.yml,
# checks the tarball against SHA256SUMS, and installs the `stitch` binary.
#
# The binary is installed into /usr/local/bin when run as root, and otherwise
# into $XDG_BIN_HOME, or ~/.local/bin if that is unset.
#
# STITCH_RELEASES_URL overrides the base URL of the releases (by default,
# https://github.com/nelhage/stitch/releases); it is for testing, or for a
# mirror with the same layout.

# Everything is inside main(), so that a truncated download (when piped into
# sh) cannot run a partial script.

set -eu

usage() {
    cat <<EOF
Usage: install.sh [--version VERSION] [--dir DIR]

Download a release of stitch for this platform, and install the binary.

  --version VERSION  The release to install, e.g. v0.1 (default: latest).
  --dir DIR          The directory to install into (default: /usr/local/bin
                     when run as root; otherwise \$XDG_BIN_HOME, or
                     ~/.local/bin if that is unset).
  -h, --help         Show this help.
EOF
}

say() {
    echo "stitch-install: $*" >&2
}

die() {
    say "error: $*"
    exit 1
}

# The release target for this machine, as named in the release tarballs.
detect_target() {
    os=$(uname -s)
    arch=$(uname -m)
    case "$os" in
        Linux)
            # The Linux builds are static, so they need only a kernel of the
            # right architecture, whatever the userspace.
            case "$arch" in
                x86_64 | amd64) echo x86_64-unknown-linux-musl ;;
                aarch64 | arm64) echo aarch64-unknown-linux-musl ;;
                *) die "no release build for Linux on $arch" ;;
            esac
            ;;
        Darwin)
            case "$arch" in
                arm64) echo aarch64-apple-darwin ;;
                x86_64)
                    # A shell running under Rosetta on Apple Silicon reports
                    # x86_64; the native build runs there.
                    if [ "$(sysctl -n sysctl.proc_translated 2>/dev/null || true)" = 1 ]; then
                        echo aarch64-apple-darwin
                    else
                        die "no release build for Intel Macs"
                    fi
                    ;;
                *) die "no release build for macOS on $arch" ;;
            esac
            ;;
        *) die "no release build for $os" ;;
    esac
}

# download URL FILE
download() {
    url=$1
    out=$2
    if command -v curl >/dev/null 2>&1; then
        # Refuse to be redirected away from https.
        case "$url" in
            https://*) set -- --proto '=https' ;;
            *) set -- ;;
        esac
        curl --fail --silent --show-error --location --retry 3 \
            "$@" --output "$out" -- "$url"
    elif command -v wget >/dev/null 2>&1; then
        wget --quiet --output-document="$out" -- "$url"
    else
        die "neither curl nor wget is installed"
    fi
}

# sha256 FILE: print the SHA-256 of FILE, in hex.
sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | cut -d' ' -f1
    elif command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 -r "$1" | cut -d' ' -f1
    else
        die "no sha256sum, shasum or openssl to verify the download with"
    fi
}

main() {
    version=latest
    dir=
    while [ $# -gt 0 ]; do
        case "$1" in
            --version)
                [ $# -ge 2 ] || die "--version needs an argument"
                version=$2
                shift
                ;;
            --version=*) version=${1#--version=} ;;
            --dir)
                [ $# -ge 2 ] || die "--dir needs an argument"
                dir=$2
                shift
                ;;
            --dir=*) dir=${1#--dir=} ;;
            -h | --help)
                usage
                exit 0
                ;;
            *)
                usage >&2
                exit 2
                ;;
        esac
        shift
    done

    # The release, as a path under the releases URL. Tags are v0.x; accept
    # the version without its `v`, too.
    case "$version" in
        "" | *[!A-Za-z0-9._+-]*) die "invalid version '$version'" ;;
        latest) release=latest/download ;;
        [0-9]*) release=download/v$version ;;
        *) release=download/$version ;;
    esac

    if [ -z "$dir" ]; then
        if [ "$(id -u)" = 0 ]; then
            dir=/usr/local/bin
        elif [ -n "${XDG_BIN_HOME:-}" ]; then
            dir=$XDG_BIN_HOME
        else
            [ -n "${HOME:-}" ] || die "HOME is not set; pass --dir"
            dir=$HOME/.local/bin
        fi
    fi
    # `--dir=~/bin` reaches us unexpanded.
    case "$dir" in
        "~") dir=$HOME ;;
        "~/"*) dir=$HOME/${dir#"~/"} ;;
    esac
    case "$dir" in
        /*) ;;
        *) dir=$PWD/$dir ;;
    esac
    dir=${dir%/}

    target=$(detect_target)
    asset=stitch-$target.tar.gz
    base=${STITCH_RELEASES_URL:-https://github.com/nelhage/stitch/releases}/$release

    tmp=$(mktemp -d "${TMPDIR:-/tmp}/stitch-install.XXXXXX")
    staged=
    trap 'rm -rf "$tmp"; [ -z "$staged" ] || rm -f "$staged"' EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    say "downloading $base/$asset"
    download "$base/$asset" "$tmp/$asset" ||
        die "could not download $asset; does release '$version' exist, with a build for $target?"
    download "$base/SHA256SUMS" "$tmp/SHA256SUMS" ||
        die "could not download SHA256SUMS for release '$version'"

    expected=$(awk -v f="$asset" '$2 == f || $2 == "*" f { print $1 }' "$tmp/SHA256SUMS")
    [ -n "$expected" ] || die "SHA256SUMS has no entry for $asset"
    actual=$(sha256 "$tmp/$asset")
    [ "$actual" = "$expected" ] ||
        die "checksum mismatch for $asset: expected $expected, got $actual"

    mkdir -p "$tmp/x"
    tar -xzf "$tmp/$asset" -C "$tmp/x" stitch ||
        die "could not extract stitch from $asset"

    mkdir -p "$dir" || die "could not create $dir"
    [ -w "$dir" ] || die "$dir is not writable; run as root, or pass --dir"

    # Stage the binary next to its destination, and rename it into place.
    # Writing over the old binary instead would fail ("text file busy"), or on
    # macOS corrupt its code signature, while it is running, as a stitch server
    # may well be; a rename leaves running processes with the old file.
    staged=$dir/.stitch.install.$$
    cp "$tmp/x/stitch" "$staged"
    chmod 755 "$staged"
    # Make sure it runs before replacing whatever is installed.
    installed_version=$("$staged" --version) ||
        die "the downloaded binary does not run on this machine"
    mv -f "$staged" "$dir/stitch"
    staged=

    say "installed $installed_version to $dir/stitch"

    case ":${PATH:-}:" in
        *":$dir:"* | *":$dir/:"*)
            found=$(command -v stitch || true)
            if [ "$found" != "$dir/stitch" ]; then
                say "warning: \`stitch\` on your PATH is $found, not $dir/stitch"
            fi
            ;;
        *)
            say "warning: $dir is not on your PATH; add it in your shell's startup file, e.g.:"
            say "    export PATH=\"$dir:\$PATH\""
            ;;
    esac
    say "note: when connecting to this host, stitch runs \`stitch\` here through a"
    say "non-interactive ssh session, whose PATH may be more minimal than your shell's."
    say "Check that \`ssh THIS-HOST stitch --version\` works,"
    say "or pass --remote-stitch=$dir/stitch to the client."
}

main "$@"
