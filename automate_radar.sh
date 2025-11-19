#!/bin/bash
cd /home/gmr-lab/Desktop/radar_home/

# Update git repository
git pull

# Activate environment
source /home/gmr-lab/Desktop/radar_home/env_home/bin/activate

# Start the program with daily log file
LOGFILE="/home/gmr-lab/Desktop/radar_home/meas/log_$(date +%Y%m%d).txt"
nohup python3 cw_multinode.py > "$LOGFILE" 2>&1 &