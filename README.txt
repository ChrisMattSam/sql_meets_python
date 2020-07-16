Author: Christopher Sampah
Email: christopher.m.sampah@gmail.com
Date: June-July 2020

The goal is to automate the pushing of a metadata table in a shared folder to a table within a SQL database.  What actually accomplishes this task are Python (.py) scripts.  Utilizing Windows Task Scheduler, I then schedule the automatic running of these scripts, for which batch (.bat) files were created.  At the scheduled times of day, the local machine of these scripts executes the batch files which run the Python scripts.