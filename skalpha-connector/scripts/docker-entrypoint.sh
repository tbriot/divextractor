#!/bin/bash

# Start cron and syslog daemon
service rsyslog start
cron -f