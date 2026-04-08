#!/bin/bash

echo "======================================================="
echo "  HUCM OFD Impact Dashboard — Build & Deploy"
echo "======================================================="
echo ""
echo "Scanning for new Zoom CSV files and analyzing data..."
echo ""

# Get the directory where this script sits
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# Run the python engine
python3 preprocess.py

echo ""
echo "----------------------------------------------------"
echo "Pipeline execution finished."
echo "If this folder is synced with GitHub or Netlify, the "
echo "live website will update automatically within 15 seconds."
echo "----------------------------------------------------"
echo ""
read -p "Press any key to close this window..."
