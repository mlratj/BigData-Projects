Yellow Taxi project had been prepared as a part of my study program, more 
specificly the BigData course. It utilizes GoogleCloudPlatform solution to run 
a cluster with Hadoop on it, that makes using MapReduce and Hive technology 
feasible.  
  
Processed data comes from 
[The New York City Taxi and Limousine Commission (TLC) website](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). In-deep explanation of the data set can be found 
[here](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).  
  
My project was designed to handle coping files between Google Storage, local 
cluster's path and HDFS filesystem.  
  
The idea was to transform data using MapReduce and push its output to Hive database 
to perform further ELT process. In Hive data are enriched with extra information 
by simple mapping and the top pickup areas of NeyYork for a given period of time 
are selected. Moreover there is one limitation. Stakeholder requested to only 
count rides paid by cash, thus a filter in MapReduce (java code) was implemented.

