#!/usr/bin/env sh

export PORT=${PORT:-4443}
export RUST_LOG=${RUST_LOG:-info}

mkdir cert
# Nothing to see here...
echo "$MOQ_CRT" | base64 -d > dev/moq-demo.crt
echo "$MOQ_KEY" | base64 -d > dev/moq-demo.key

# Set up qlog directory if QLOG_DIR is set
QLOG_ARGS=""
if [ -n "${QLOG_DIR}" ]; then
    mkdir -p "${QLOG_DIR}"
    QLOG_ARGS="--qlog-dir ${QLOG_DIR} --qlog-serve"
    echo "qlog enabled: writing to ${QLOG_DIR}, serving at /qlog/:cid"
fi

# Set up mlog directory if MLOG_DIR is set
MLOG_ARGS=""
if [ -n "${MLOG_DIR}" ]; then
    mkdir -p "${MLOG_DIR}"
    MLOG_ARGS="--mlog-dir ${MLOG_DIR} --mlog-serve"
    echo "mlog enabled: writing to ${MLOG_DIR}, serving at /mlog/:cid"
fi

# Enable dev mode if either qlog or mlog serving is enabled
DEV_ARGS=""
if [ -n "${QLOG_DIR}" ] || [ -n "${MLOG_DIR}" ]; then
    DEV_ARGS="--dev"
fi

moq-relay-ietf --bind "[::]:${PORT}" --tls-cert dev/moq-demo.crt --tls-key dev/moq-demo.key ${DEV_ARGS} ${QLOG_ARGS} ${MLOG_ARGS}
