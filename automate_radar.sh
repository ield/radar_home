#!/bin/bash

# Go to project directory
cd /home/gmr-lab/Desktop/radar_home/ || exit 1

# Update git repository
git pull

# Activate Python environment
source /home/gmr-lab/Desktop/radar_home/env_home/bin/activate

# Create daily log file
LOGFILE="/home/gmr-lab/Desktop/radar_home/meas/log_$(date +%Y%m%d).txt"

# Run Python script in background with unbuffered output
nohup python3 -u cw_multinode.py > "$LOGFILE" 2>&1 < /dev/null &