#!/usr/bin/env bash

set -e

if [[ -z "$GITHUB_TOKEN" ]]; then
  echo "Set the GITHUB_TOKEN env variable."
  exit 1
fi

if [[ -z "$ASSET_NAME_PREFIX" ]]; then
  echo "Set the ASSET_NAME_PREFIX env variable."
  exit 1
fi

if [[ -z "$ASSET_DIR" ]]; then
  echo "Set the ASSET_DIR env variable."
  exit 1
fi

VERSION=$1
if [[ -z "$VERSION" ]]; then
  echo "Set the VERSION parameter."
  exit 1
fi

_DIR=$(dirname "$0")
UPLOAD_URL=$($_DIR/get_draft_release.sh UPLOAD_URL)

upload_asset() {
    local file=$1
    local size=$2
    local content_type=$3
    echo "Uploading $file ($size bytes) to $UPLOAD_URL"
    curl \
      -H "Authorization: token $GITHUB_TOKEN" \
      -H "Content-Length: $size"\
      -H "Content-Type: $content_type" \
      --data-binary @$file "$UPLOAD_URL?name=$(basename $file)"
}

EXTENSION=".zip"
FILE=$ASSET_DIR/$ASSET_NAME_PREFIX$VERSION$EXTENSION
# Skip if zip files do not exist (some extensions do not generate examples in zip format)
if [[ ! -f "$FILE" && "$FILE" != *".zip" ]]; then
    echo "$FILE does not exist."
fi
SIZE=$(wc -c $FILE | awk '{print $1}')
if [[ $SIZE -eq 0 ]]; then
    echo "$FILE is empty."
fi
MIME=$(file -b --mime-type $FILE)
upload_asset $FILE $SIZE $MIME


