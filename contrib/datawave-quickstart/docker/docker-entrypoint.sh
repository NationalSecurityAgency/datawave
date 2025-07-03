#!/usr/bin/env bash

/usr/bin/nohup /usr/sbin/sshd -D > /dev/null 2>&1 &

bash -c "$@"
