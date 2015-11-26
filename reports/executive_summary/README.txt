To Deploy
=========

1. Run 'package.sh' to create executive-report-v4-0.X.tar.gz
2. Log in to Telemetry Self-Serve Data Analysis
3. Click 'Schedule a job'
4. Edit or create a job with the following parameters:
  4a. Weekly:
    Job Name:              executive-report-weekly
    Code Tarball:          Upload executive-report-v4-0.X.tar.gz
    Execution Commandline: ./run.sh weekly
    Output Directory:      output
    Output Visibility:     Private
    Schedule Frequency:    Weekly
    Day of Week:           Monday
    Day of Month:          n/a (1)
    Time of Day (UTC):     10am
    Job Timeout (minutes): 120
  4b. Monthly:
    Job Name:              executive-report-monthly
    Code Tarball:          Upload executive-report-v4-0.X.tar.gz
    Execution Commandline: ./run.sh monthly
    Output Directory:      output
    Output Visibility:     Private
    Schedule Frequency:    Monthly
    Day of Week:           n/a (Sunday)
    Day of Month:          1 (It will run for the previous month)
    Time of Day (UTC):     10am
    Job Timeout (minutes): 240
