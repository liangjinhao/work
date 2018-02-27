#!/usr/bin/env bash

PID_FILE="pid_file"

# run
if [ "$1" = "stop" ]; then
    pid=$(cat $PID_FILE)
    kill -9 $pid
else
    if [ -f "/usr/bin/python3" ]; then
        nohup /usr/bin/python3 service.py --port=1999 --log_file_prefix=tornado_1999.log "$@" > nohup.log 2>&1 &
    else
        nohup python3 service.py --port=1999 --log_file_prefix=tornado_1999.log "$@" > nohup.log 2>&1 &
    fi
    echo $! > $PID_FILE
fi
