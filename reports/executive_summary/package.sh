#!/bin/bash

VERSION=0.11

wget https://raw.githubusercontent.com/mozilla/firefox-executive-dashboard/master/data/reformat_v4.py

tar czvf executive-report-v4-${VERSION}.tar.gz run.sh run_executive_report.py reformat_v4.py
