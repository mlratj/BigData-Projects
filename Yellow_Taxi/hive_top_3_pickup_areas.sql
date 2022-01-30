CREATE DATABASE NewYork;
USE NewYork;
CREATE TABLE cash_rides(
  month int, 
  pick_up_id int,
  people_count int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
;

LOAD DATA LOCAL INPATH '/home/mich_nauka/mr_to_hive.csv' INTO TABLE cash_rides;

CREATE TABLE cash_rides_orc(
  month int,
  pick_up_id int,
  people_count int
) STORED AS ORC;

INSERT INTO cash_rides_orc
SELECT * FROM  cash_rides
WHERE month IS NOT NULL;

-- areas lookup

CREATE EXTERNAL TABLE look_up_area(
  area_id int,
  borough_name string,
  zone string,
  service_zone string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '/user/mich_nauka/zones';

CREATE TABLE IF NOT EXISTS areas_orc(
  area_id int,
  borough_name string,
  zone string,
  service_zone string
) COMMENT 'look up for NY taxi zones'
STORED AS ORC;

INSERT OVERWRITE TABLE areas_orc
SELECT * FROM look_up_area
WHERE area_id IS NOT NULL;

CREATE EXTERNAL TABLE newyork.table_csv_export_data(
  month int,
  borough string,
  people_sum int,
  rank int
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '/user/mich_nauka/final';

INSERT OVERWRITE TABLE newyork.table_csv_export_data
SELECT
  t1.month as month,
  t1.borough_name as borough,
  t1.people_sum as people_sum,
  t1.rank as rank
FROM (
  SELECT 
  t0.month,
  t0.borough_name,
  t0.people_sum,
  RANK() OVER (PARTITION BY t0.month ORDER BY t0.people_sum DESC) as rank
  FROM (
    SELECT
      m.month,  
      l.borough_name,  
      SUM(m.people_count) as people_sum
    FROM newyork.cash_rides_orc m
    INNER JOIN newyork.areas_orc l ON m.pick_up_id = l.area_id
    GROUP BY
      m.month,  
      l.borough_name
      ) t0
  ) t1
  WHERE t1.rank <= 3
;
