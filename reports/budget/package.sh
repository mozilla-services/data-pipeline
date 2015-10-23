#!/bin/bash

VERSION=0.3

tar czvf budget-report-${VERSION}.tar.gz budget.toml run.sh schema_template.json check_targets.py
