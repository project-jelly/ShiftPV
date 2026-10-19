#!/bin/sh
set -eu

if [ "${1:-}" = copy ] && [ -f /pool/.shiftpv-e2e-fail-copy ]; then
	echo 'injected copy failure before filesystem mutation' >&2
	exit 97
fi
if [ "${1:-}" = copy ] && [ -f /pool/.shiftpv-e2e-delay-copy-completion ]; then
	/shiftpv-volume-helper-real "$@"
	echo 'injected completion delay after real verified copy' >&2
	while [ -f /pool/.shiftpv-e2e-delay-copy-completion ]; do
		sleep 1
	done
	exit 0
fi
exec /shiftpv-volume-helper-real "$@"
