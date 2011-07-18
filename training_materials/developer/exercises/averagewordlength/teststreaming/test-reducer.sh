#!/bin/bash

set -o errexit

if [ $# != 1 ]; then
	echo "Usage: $0 reducer-script"
	exit 1
fi

REDUCER_OUTPUT=$(mktemp)

cat <<EOF | $1 > $REDUCER_OUTPUT
c	5
g	7
h	3
h	5
w	5
w	5
W	5
EOF

cat <<'EOF' | diff -u $REDUCER_OUTPUT - && echo pass || echo fail
c	5.0
g	7.0
h	4.0
w	5.0
W	5.0
EOF

