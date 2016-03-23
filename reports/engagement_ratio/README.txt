To Deploy
=========

1. Log in to Telemetry Self-Serve Data Analysis
2. Click 'Schedule a Spark Job'
3. Edit or create a job with the following parameters:
    Job Name:              telemetry-engagement-ratio
    Notebook or Jar:       Upload MauDau.ipynb
    Spark Submission Args: N/A
    Cluster Size:          10
    Output Visibility:     Public
    Schedule Frequency:    Daily
    Day of Week:           N/A (Sunday)
    Day of Month:          N/A (1)
    Time of Day (UTC):     4am
    Job Timeout (minutes): 300
