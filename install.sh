#!/usr/bin/env bash
# TorrentSeek installer for Linux, macOS, and Android (Termux).
#
#   curl -fsSL https://raw.githubusercontent.com/Jonny-GM/TorrentSeek/main/install.sh | bash
#
# While the repository is private the raw URL above 404s; fetch and run
# through the gh CLI (https://cli.github.com, after `gh auth login`)
# instead — the script then also downloads the release itself through gh:
#
#   gh api -H "Accept: application/vnd.github.raw" repos/Jonny-GM/TorrentSeek/contents/install.sh | bash
#
# Options:
#   --version vX.Y.Z        install a specific release (default: newest
#                           versioned release, falling back to rolling)
#   --latest                install the rolling "latest" prerelease
#   --prefix DIR            install the binary into DIR (default:
#                           /usr/local/bin, ~/.local/bin without sudo,
#                           $PREFIX/bin on Termux)
#   --service               start at login (systemd user unit on Linux,
#                           LaunchAgent on macOS)
#   --uninstall             remove the binary and any service files
#
# The script downloads the release archive for your platform, verifies its
# SHA-256 against the release's sha256sums.txt, and installs the single
# torrentseek binary. It does not install or configure Deluge.
set -euo pipefail

REPO="Jonny-GM/TorrentSeek"
API="https://api.github.com/repos/$REPO"
DL="https://github.com/$REPO/releases/download"
UNIT_PATH="$HOME/.config/systemd/user/torrentseek.service"
PLIST_PATH="$HOME/Library/LaunchAgents/dev.torrentseek.daemon.plist"

log() { printf '>> %s\n' "$*"; }
die() { printf 'install.sh: error: %s\n' "$*" >&2; exit 1; }

VERSION="" ROLLING=0 SERVICE=0 UNINSTALL=0 PREFIX_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) VERSION="${2:?--version needs an argument}"; shift 2 ;;
    --latest) ROLLING=1; shift ;;
    --prefix) PREFIX_DIR="${2:?--prefix needs an argument}"; shift 2 ;;
    --service) SERVICE=1; shift ;;
    --uninstall) UNINSTALL=1; shift ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) die "unknown option $1 (see --help)" ;;
  esac
done

command -v curl >/dev/null || die "curl is required"

# Release downloads go through the gh CLI whenever it's installed and
# authenticated: mandatory while the repository is private (unauthenticated
# requests to a private repo's releases return 404), a free rate-limit
# bump once it's public.
GH=0
if command -v gh >/dev/null && gh auth status >/dev/null 2>&1; then
  GH=1
fi

# Prints the newest versioned (vX.Y.Z) release tag, or nothing if none
# exists. GitHub's releases/latest endpoint never returns prereleases, so
# the rolling "latest" build can't satisfy this and the caller falls back
# to it explicitly.
latest_versioned_tag() {
  if [[ "$GH" == 1 ]]; then
    gh api "repos/$REPO/releases/latest" --jq .tag_name 2>/dev/null | grep '^v' || true
  else
    curl -fsSL "$API/releases/latest" 2>/dev/null \
      | grep -o '"tag_name" *: *"[^"]*"' | head -1 | sed 's/.*"\(v[^"]*\)".*/\1/' || true
  fi
}

# fetch_asset TAG NAME DIR: download one release asset into DIR.
fetch_asset() {
  if [[ "$GH" == 1 ]]; then
    gh release download "$1" --repo "$REPO" --pattern "$2" --dir "$3" --clobber
  else
    curl -fsSL --retry 3 "$DL/$1/$2" -o "$3/$2"
  fi
}

# --- platform detection ------------------------------------------------------

os="$(uname -s)"
arch="$(uname -m)"
case "$arch" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) die "unsupported architecture: $arch" ;;
esac

termux=0
if [[ -n "${PREFIX:-}" && "$PREFIX" == *com.termux* ]]; then
  termux=1
  [[ "$arch" == arm64 ]] || die "Termux install supports arm64 phones only"
  platform="android-termux_arm64"
elif [[ "$os" == Linux ]]; then
  platform="linux_$arch"
elif [[ "$os" == Darwin ]]; then
  platform="macos_$arch"
else
  die "unsupported OS: $os (use install.ps1 on Windows)"
fi

# --- install destination -----------------------------------------------------

SUDO=""
if [[ -n "$PREFIX_DIR" ]]; then
  bindir="$PREFIX_DIR"
elif [[ "$termux" == 1 ]]; then
  bindir="$PREFIX/bin"
elif [[ -w /usr/local/bin ]]; then
  bindir=/usr/local/bin
elif command -v sudo >/dev/null; then
  bindir=/usr/local/bin
  SUDO=sudo
else
  bindir="$HOME/.local/bin"
fi

# --- uninstall ---------------------------------------------------------------

if [[ "$UNINSTALL" == 1 ]]; then
  if [[ "$os" == Linux && -f "$UNIT_PATH" ]]; then
    systemctl --user disable --now torrentseek 2>/dev/null || true
    rm -f "$UNIT_PATH"
    log "removed systemd user unit"
  fi
  if [[ "$os" == Darwin && -f "$PLIST_PATH" ]]; then
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
    rm -f "$PLIST_PATH"
    log "removed LaunchAgent"
  fi
  if [[ -e "$bindir/torrentseek" ]]; then
    $SUDO rm -f "$bindir/torrentseek"
    log "removed $bindir/torrentseek"
  else
    log "no binary at $bindir/torrentseek"
  fi
  if [[ -e "$bindir/torrentprobe" ]]; then
    $SUDO rm -f "$bindir/torrentprobe"
    log "removed $bindir/torrentprobe"
  fi
  exit 0
fi

# --- resolve release tag -----------------------------------------------------

if [[ "$ROLLING" == 1 ]]; then
  tag=latest
elif [[ -n "$VERSION" ]]; then
  tag="$VERSION"
else
  tag="$(latest_versioned_tag)"
  if [[ -z "${tag:-}" ]]; then
    log "no versioned release found; installing the rolling latest build"
    tag=latest
  fi
fi

# --- download and verify -----------------------------------------------------

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

log "fetching checksums for release '$tag'"
fetch_asset "$tag" sha256sums.txt "$tmp" \
  || die "release '$tag' not found (or it has no sha256sums.txt). If the repository is private, install and authenticate the gh CLI first (gh auth login)"

artifact="$(awk '{print $2}' "$tmp/sha256sums.txt" | grep -F "_${platform}." | head -1 || true)"
[[ -n "$artifact" ]] || die "release '$tag' has no artifact for $platform"

log "downloading $artifact"
fetch_asset "$tag" "$artifact" "$tmp"

grep -F "  $artifact" "$tmp/sha256sums.txt" > "$tmp/one.sum"
if command -v sha256sum >/dev/null; then
  (cd "$tmp" && sha256sum -c one.sum >/dev/null) || die "checksum mismatch for $artifact"
else
  (cd "$tmp" && shasum -a 256 -c one.sum >/dev/null) || die "checksum mismatch for $artifact"
fi
log "checksum verified"

tar -xzf "$tmp/$artifact" -C "$tmp"
bin="$(find "$tmp" -type f -name torrentseek | head -1)"
[[ -n "$bin" ]] || die "archive did not contain a torrentseek binary"

[[ -n "$SUDO" ]] && log "installing to $bindir (needs sudo)"
$SUDO mkdir -p "$bindir"
$SUDO install -m 0755 "$bin" "$bindir/torrentseek"
log "installed $("$bindir/torrentseek" -version 2>/dev/null || echo torrentseek) → $bindir/torrentseek"

# torrentprobe (the fetch/verify diagnostic tool) ships alongside torrentseek
# in newer releases; older archives won't have it, so this is best-effort.
probe="$(find "$tmp" -type f -name torrentprobe | head -1)"
if [[ -n "$probe" ]]; then
  $SUDO install -m 0755 "$probe" "$bindir/torrentprobe"
  log "installed torrentprobe → $bindir/torrentprobe"
fi

case ":$PATH:" in
  *":$bindir:"*) ;;
  *) log "note: $bindir is not on your PATH" ;;
esac

# --- optional login service --------------------------------------------------

if [[ "$SERVICE" == 1 ]]; then
  if [[ "$termux" == 1 ]]; then
    log "no service setup on Termux; run '$bindir/torrentseek' manually (or use termux-services)"
  elif [[ "$os" == Linux ]]; then
    mkdir -p "$(dirname "$UNIT_PATH")"
    cat > "$UNIT_PATH" <<EOF
[Unit]
Description=TorrentSeek daemon
After=network.target

[Service]
ExecStart=$bindir/torrentseek
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
    systemctl --user daemon-reload
    systemctl --user enable --now torrentseek
    log "systemd user unit enabled (survives logout only with: loginctl enable-linger $USER)"
  elif [[ "$os" == Darwin ]]; then
    mkdir -p "$(dirname "$PLIST_PATH")"
    cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>dev.torrentseek.daemon</string>
  <key>ProgramArguments</key><array><string>$bindir/torrentseek</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
</dict></plist>
EOF
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
    launchctl load "$PLIST_PATH"
    log "LaunchAgent loaded (starts at login)"
  fi
fi

log "done. TorrentSeek needs deluged (Deluge 2.0+) with the PiecePriority plugin"
log "reachable from this host -- see the README's Requirements. Then:"
log "torrentseek -deluge-user <user> -deluge-pass <pass>    (API on http://127.0.0.1:3480, see torrentseek -h)"
