#!/usr/bin/env bash
# Builds the zeno-exec release binaries for darwin and linux on amd64
# and arm64, writes SHA256SUMS next to them, and prints both.
#
# usage: scripts/release-zeno-exec.sh <version>
set -euo pipefail

GO_VERSION="1.23.12"

version="${1:-}"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: $0 <major.minor.patch>" >&2
  exit 2
fi

cd "$(dirname "$0")/.."

export GOTOOLCHAIN="go${GO_VERSION}"
if [[ "$(go version)" != "go version go${GO_VERSION} "* ]]; then
  echo "expected go ${GO_VERSION}, got: $(go version)" >&2
  exit 1
fi

out="dist/zeno-exec-v${version}"
rm -rf "$out"
mkdir -p "$out"

for os in darwin linux; do
  for arch in amd64 arm64; do
    CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath \
      -ldflags "-X main.version=${version}" \
      -o "${out}/zeno-exec-${os}-${arch}" ./cmd/zeno-exec
  done
done

cd "$out"
if command -v shasum >/dev/null 2>&1; then
  LC_ALL=C shasum -a 256 zeno-exec-* | LC_ALL=C sort -k 2 > SHA256SUMS
else
  LC_ALL=C sha256sum zeno-exec-* | LC_ALL=C sort -k 2 > SHA256SUMS
fi

ls -l zeno-exec-*
cat SHA256SUMS
