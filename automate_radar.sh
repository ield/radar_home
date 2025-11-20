#!/bin/bash

#Generate a log file to print the output
exec > /home/gmr-lab/Desktop/radar_home/meas/debug.log 2>&1
set -x

# Go to project directory
cd /home/gmr-lab/Desktop/radar_home/ || exit 1

# Update git repository
echo "Pulling git ..."
git pull

# Activate Python environment
echo "Activating env..."
source /home/gmr-lab/Desktop/radar_home/env_home/bin/activate

#Check the python version
which python3
python3 --version

# Create daily log file
echo "Running Python Script..."
LOGFILE="/home/gmr-lab/Desktop/radar_home/meas/log_$(date +%Y%m%d).txt"
# Run Python script in background with unbuffered output
exec python3 cw_multinode.py >> "$LOGFILE" 2>&1

echo "Process completed"
