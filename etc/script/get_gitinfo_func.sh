#!/bin/bash

function get_gitinfo() {
    local BUILD_TIME
    local COMMIT_ID
    local SHORT_COMMIT_ID

    BUILD_TIME=$(git log -1 --format=%ai 2>/dev/null || echo "unknown")
    if [ "$BUILD_TIME" != "unknown" ]; then
        BUILD_TIME=${BUILD_TIME:0:10}
    fi

    COMMIT_ID=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
    SHORT_COMMIT_ID=$([ "$COMMIT_ID" != "unknown" ] && echo ${COMMIT_ID:0:8} || echo "unknown")

    if [ "$SHORT_COMMIT_ID" = "unknown" ]; then
        echo "no git commit id"
        SHORT_COMMIT_ID="kiwi"
    fi

    echo "$BUILD_TIME $SHORT_COMMIT_ID"
}