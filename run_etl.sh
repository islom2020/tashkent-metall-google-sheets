#!/bin/bash

# Navigate to the script's directory
cd /home/upsoft/projects/tashkent-metall-google-sheets

# Activate the virtual environment
source .venv/bin/activate

# Run the ETL Python script
python3 run_etls.py