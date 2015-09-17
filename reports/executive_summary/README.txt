To Deploy
=========

1. Run 'package.sh' to create executive-report-v4-0.X.tar.gz
2. Log in to Telemetry Self-Serve Data Analysis
3. Click 'Schedule a job'
4. Edit or create a job with the following parameters:
    Job Name:              executive-report-v4
    Code Tarball:          Upload executive-report-v4-0.X.tar.gz
    Execution Commandline: ./run.sh
    Output Directory:      output
    Output Visibility:     Private
    Schedule Frequency:    Daily
    Day of Week:           n/a (Sunday)
    Day of Month:          n/a (1)
    Time of Day (UTC):     2pm
    Job Timeout (minutes): 120
