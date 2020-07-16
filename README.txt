Author: Christopher Sampah
Email: christopher.m.sampah@gmail.com
Date: June-July 2020

The goal is to automate the syncing of a metadata table in an Excel document into a table within a SQL database.  What actually accomplishes this task are Python (.py) scripts.  Utilizing Windows Task Scheduler, the daily running of these Python scripts are automated, the purpose for which batch (.bat) files were created.  At the scheduled times of day, the local machine on which these scripts are housed executes the batch files which run the Python scripts.