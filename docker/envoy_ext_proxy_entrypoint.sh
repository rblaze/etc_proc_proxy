#!/bin/sh

case "$USE_DDTRACE" in
    [Tt][Rr][Uu][Ee])
        export DD_TRACE_OPENAI_ENABLED="False"
        exec ddtrace-run ext-proc-proxy "$@"
        ;;
esac

exec ext-proc-proxy "$@"
