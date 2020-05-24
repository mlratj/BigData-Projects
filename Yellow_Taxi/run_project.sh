#!/bin/bash
: <<'GPC' 
Creating a cluster on GCP
gcloud beta dataproc clusters create mycluster \
--enable-component-gateway --bucket wsb_lab \
--region europe-west3 --subnet default --zone europe-west3-c \
--master-machine-type n1-standard-2 --master-boot-disk-size 50 \
--num-workers 2 \
--worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
--image-version 1.3-deb9 \
--optional-components ZEPPELIN \
--project labbigdataprocessing --max-age=1h
GPC

echo "Welcome to my project utilizing MapReduce and Hive technology!"
# cleans working space
find . \! -name 'run_project.sh' -delete 
hive -e "DROP DATABASE IF EXISTS NewYork CASCADE;"
# imports MapReduce script and raw data from Google Storage (part of GCP)
hadoop fs -copyToLocal gs://wsb_lab/project/passengercount.jar
hadoop fs -copyToLocal gs://wsb_lab/project/input/*
# concatenates given files skipping 2nd file header
cat yellow_tripdata_2018-11.csv <(tail -q -n +3 yellow_tripdata_2018-12.csv) > 2018_11_and_12.csv
# instantly cleans space
rm yellow_tripdata_2018-11.csv
rm yellow_tripdata_2018-12.csv
# runs MapReduce and a few operations on HDFS FS scace tasks
hadoop jar passengercount.jar Main
hadoop fs -copyToLocal */output/*
# concatenates MapReduce output
cat part* > all_parts.csv
echo "month,location,passanger_count" > mr_header.csv
cat mr_header.csv all_parts.csv > all_with_header.csv
cat all_with_header.csv | tr -d "[:blank:]" >> mr_to_hive.csv
# saves MapReduce output in Google Store
gsutil cp part* gs://wsb_lab/output
hadoop fs -mkdir zones
hadoop fs -copyToLocal gs://wsb_lab/project/taxi_zone_lookup.csv
hadoop fs -copyFromLocal taxi_zone_lookup.csv zones/
hadoop fs -copyToLocal gs://wsb_lab/project/hive_top_3_pickup_areas.sql
hadoop fs -mkdir final
# processing in Hive
hive -f hive_top_3_pickup_areas.sql
mkdir hive_output
hadoop fs -copyToLocal /user/mich_nauka/final/* hive_output/
# concatenates Hive output
cat /home/mich_nauka/hive_output/* > hive_final.csv
# (optional as Hive can give json on output) converts Hive output (csv) to json
hadoop fs -copyToLocal gs://wsb_lab/project/csv_to_json.py
python3 csv_to_json.py
# ask a user for reading an output
while true; do
    read -p $"Do you wish to read the output? (y/n)    " yn
    case $yn in
        [Yy]* ) cat hive_final.json; break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done
# copies final json output (of created) to Google Storage
gsutil cp hive_final.json gs://wsb_lab/output
# cleans up a working space (dangerous!)
rm *
rm -R *
hadoop fs -rm -R *
