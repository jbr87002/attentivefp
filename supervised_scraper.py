#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Supervised script to run the NIST Spectra Scraper with automatic restart on failure."""
import os
import sys
import time
import subprocess
import signal
import re
from datetime import datetime, timedelta
from collections import deque

# Configuration
SCRAPER_SCRIPT = "1) NIST Spectra Scraper.py"
LOG_FILE = "scraping_output.log"
RESTART_DELAY = 60  # seconds to wait before restarting
MAX_RESTARTS = 10000   # maximum number of restart attempts
FAILURE_THRESHOLD = (20, 25) # (failures, minutes): if 20 failures in 25 minutes, stop script

def log_message(message):
    """Log a message with timestamp to both console and log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = f"[{timestamp}] {message}"
    print(formatted_message)
    
    with open(LOG_FILE, "a") as log:
        log.write(formatted_message + "\n")

def is_failure(log_content):
    """Check if the log indicates a failure that requires restart."""
    # Common failure patterns
    failure_patterns = [
        r"Connection refused",
        r"Connection reset by peer",
        r"timeout",
        r"Too many requests",
        r"rate limit",
        r"HTTPError",
        r"SSLError",
        r"ConnectionError",
        r"Max retries exceeded",
        r"ChunkedEncodingError",
        r"ReadTimeout",
        r"ConnectTimeout"
    ]
    
    for pattern in failure_patterns:
        if re.search(pattern, log_content, re.IGNORECASE):
            return True
    
    return False

def run_scraper():
    """Run the NIST scraper script and return True if successful, False otherwise."""
    try:
        # Run the scraper script and redirect output to the log file
        process = subprocess.Popen(
            [sys.executable, SCRAPER_SCRIPT],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Stream output to both console and log file
        with open(LOG_FILE, "a") as log:
            while True:
                line = process.stdout.readline()
                if not line and process.poll() is not None:
                    break
                if line:
                    print(line.strip())
                    log.write(line)
        
        # Check if the process completed successfully
        return_code = process.poll()
        
        # Read the log file to check for failure patterns
        with open(LOG_FILE, "r") as log:
            log_content = log.read()
        
        if return_code != 0 or is_failure(log_content):
            log_message(f"Scraper failed with return code {return_code}")
            return False
        
        log_message("Scraper completed successfully")
        return True
    
    except Exception as e:
        log_message(f"Error running scraper: {str(e)}")
        return False

def check_failure_threshold(failure_timestamps):
    """Check if the failure threshold has been exceeded."""
    max_failures, time_window_minutes = FAILURE_THRESHOLD
    
    # If we have fewer failures than the threshold, no need to check time window
    if len(failure_timestamps) < max_failures:
        return False
    
    # Get the current time
    current_time = datetime.now()
    
    # Calculate the time window in minutes
    time_window = timedelta(minutes=time_window_minutes)
    
    # Count failures within the time window
    recent_failures = sum(1 for timestamp in failure_timestamps 
                         if current_time - timestamp <= time_window)
    
    # If we have more failures than the threshold within the time window
    if recent_failures >= max_failures:
        log_message(f"Failure threshold exceeded: {recent_failures} failures in the last {time_window_minutes} minutes")
        return True
    
    return False

def main():
    """Main function to supervise the scraper with automatic restart on failure."""
    restart_count = 0
    failure_timestamps = deque(maxlen=1000)  # Store timestamps of recent failures
    
    # Create or clear the log file
    with open(LOG_FILE, "w") as log:
        log.write(f"Starting supervised scraper at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    log_message("Starting supervised scraper")
    
    while restart_count < MAX_RESTARTS:
        if run_scraper():
            log_message("Scraper completed successfully, exiting")
            return 0
        
        # Record the failure timestamp
        failure_timestamps.append(datetime.now())
        
        # Check if we've exceeded the failure threshold
        if check_failure_threshold(failure_timestamps):
            log_message(f"Too many failures in a short time period. Stopping script.")
            return 1
        
        restart_count += 1
        log_message(f"Scraper failed, restarting in {RESTART_DELAY} seconds (attempt {restart_count}/{MAX_RESTARTS})")
        
        # Wait before restarting
        time.sleep(RESTART_DELAY)
    
    log_message(f"Maximum restart attempts ({MAX_RESTARTS}) reached, giving up")
    return 1

if __name__ == "__main__":
    sys.exit(main()) 