#!/bin/bash

VERSION=0.12

if [ ! -s reformat_v4.py ]; then
    echo "Missing: reformat_v4.py"
    echo "Go get it from https://github.com/mozilla/firefox-executive-dashboard/blob/master/data/reformat_v4.py"
    exit 1
fi

tar czvf executive-report-v4-${VERSION}.tar.gz run.sh run_executive_report.py reformat_v4.py
