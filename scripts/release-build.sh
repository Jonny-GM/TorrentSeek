#!/usr/bin/env bash
# Cross-compiles torrentseek for every release platform into dist/.
#
# Usage: VERSION=v1.0.0 ./scripts/release-build.sh
#
# All targets are pure-Go static builds (CGO_ENABLED=0). The Android phone
# artifact is the static linux/arm64 binary: TorrentSeek is a headless
# daemon, and on a phone it runs under Termux, which executes plain Linux
# arm64 binaries — no NDK or APK involved.
set -euo pipefail

VERSION="${VERSION:?set VERSION (e.g. v1.0.0)}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$REPO/dist"
rm -rf "$DIST"
mkdir -p "$DIST"

# GOOS GOARCH artifact-platform-name
targets=(
  "linux   amd64 linux_amd64"
  "linux   arm64 linux_arm64"
  "windows amd64 windows_amd64"
  "darwin  amd64 macos_amd64"
  "darwin  arm64 macos_arm64"
  "linux   arm64 android-termux_arm64"
)

for t in "${targets[@]}"; do
  read -r goos goarch platform <<<"$t"
  ext=""
  [[ "$goos" == windows ]] && ext=".exe"

  stage="$DIST/torrentseek_${VERSION}_${platform}"
  mkdir -p "$stage"
  echo "building $platform (${goos}/${goarch})"
  ( cd "$REPO" && CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
      go build -trimpath -ldflags "-s -w -X main.version=$VERSION" \
      -o "$stage/torrentseek$ext" ./cmd/torrentseek )
  ( cd "$REPO" && CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
      go build -trimpath -ldflags "-s -w" \
      -o "$stage/torrentprobe$ext" ./cmd/torrentprobe )
  cp "$REPO/README.md" "$REPO/LICENSE" "$stage/"

  if [[ "$goos" == windows ]]; then
    ( cd "$DIST" && zip -qr "$(basename "$stage").zip" "$(basename "$stage")" )
  else
    tar -C "$DIST" -czf "$stage.tar.gz" "$(basename "$stage")"
  fi
  rm -r "$stage"
done

( cd "$DIST" && sha256sum -- * > sha256sums.txt )
echo
echo "dist/:"
ls -l "$DIST"
