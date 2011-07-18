#!/bin/bash

set -o errexit

if [ $# != 1 ]; then
	echo "Usage: $0 mapper-script"
	exit 1
fi

MAPPER_OUTPUT=$(mktemp)

cat <<EOF | $1 > $MAPPER_OUTPUT
hey World
goodbye cruel world
hello, world!
EOF

cat <<'EOF' | diff -u $MAPPER_OUTPUT - && echo pass || echo fail
h	3
W	5
g	7
c	5
w	5
h	5
w	5
EOF

