To Deploy
=========
Until Bug 1258685 lands, the notebook will automatically select the operating mode ("weekly" or
"monthly") based on the notebook file name. For this reason, two different Spark jobs need
to be scheduled.

Weekly aggregation
------------------

1. Log in to Telemetry Self-Serve Data Analysis
2. Click 'Schedule a Spark Job'
3. Edit or create a job with the following parameters:
    Job Name:              telemetry-fennec-dashboard-weekly
    Notebook or Jar:       summarize_csv_weekly.ipynb
    Spark Submission Args: N/A
    Cluster Size:          5
    Output Visibility:     Public
    Schedule Frequency:    Weekly
    Day of Week:           N/A (Sunday)
    Day of Month:          N/A (1)
    Time of Day (UTC):     4am
    Job Timeout (minutes): 300
    
Monthly aggregation
------------------

1. Log in to Telemetry Self-Serve Data Analysis
2. Click 'Schedule a Spark Job'
3. Edit or create a job with the following parameters:
    Job Name:              telemetry-fennec-dashboard-monthly
    Notebook or Jar:       summarize_csv_monthly.ipynb
    Spark Submission Args: N/A
    Cluster Size:          10
    Output Visibility:     Public
    Schedule Frequency:    Monthly
    Day of Week:           N/A (Sunday)
    Day of Month:          N/A (1)
    Time of Day (UTC):     4am
    Job Timeout (minutes): 300
