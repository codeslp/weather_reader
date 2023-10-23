#!/usr/bin/env python3

import subprocess

# Navigate to the project directory first
project_dir = "/Users/bfaris96/Desktop/turing-proj/weather_reader/weather_reader"
subprocess.call(["cd", project_dir])

# Activate the Poetry virtual environment
subprocess.call(["poetry", "shell"])
