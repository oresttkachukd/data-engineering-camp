-- setup
CREATE OR REPLACE EXTERNAL TABLE `dw_dataset.green_taxi_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dw_taxi_data/green_taxi_data_partitioned/*']
);

CREATE OR REPLACE TABLE `dw_dataset.green_taxi_data_nonpartitioned`
AS SELECT * FROM `dw_dataset.green_taxi_data`;

-- 1 
SELECT count(*) FROM `dw_dataset.green_taxi_data`;

-- 2
SELECT COUNT(DISTINCT(PULocationID)) FROM `dw_dataset.green_taxi_data`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `dw_dataset.green_taxi_data_nonpartitioned`;

-- 3
SELECT COUNT(1) FROM `dw_dataset.green_taxi_data_nonpartitioned` WHERE fare_amount = 0

-- 4
SELECT * from `dw_dataset.green_taxi_data` limit 10;

CREATE OR REPLACE TABLE `dw_dataset.green_taxi_data_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `dw_dataset.green_taxi_data`
);



-- TO DO
SELECT count(*) FROM  `dw_dataset.nytaxi.fhv_nonpartitioned_tripdata`
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');


SELECT count(*) FROM `dw_dataset.nytaxi.fhv_partitioned_tripdata`
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');