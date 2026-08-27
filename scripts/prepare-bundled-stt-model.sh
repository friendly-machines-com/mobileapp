#!/bin/sh
set -eu

url=$1
expected_sha256=$2
destination=$3
temporary="${destination}.part"

hash_file() {
    sha256sum "$1" | cut -d ' ' -f 1
}

mkdir -p "$(dirname "$destination")"

if [ -f "$destination" ] && [ "$(hash_file "$destination")" = "$expected_sha256" ]; then
    echo "Verified pinned local speech model ($(basename "$destination"))"
    exit 0
fi

trap 'rm -f "$temporary"' EXIT HUP INT TERM
rm -f "$temporary"
echo "Fetching pinned local speech model ($(basename "$destination"))"
curl --fail --location --retry 3 --connect-timeout 30 --output "$temporary" "$url"

actual_sha256=$(hash_file "$temporary")
if [ "$actual_sha256" != "$expected_sha256" ]; then
    echo "Speech model checksum mismatch: expected $expected_sha256, got $actual_sha256" >&2
    exit 1
fi

mv -f "$temporary" "$destination"
