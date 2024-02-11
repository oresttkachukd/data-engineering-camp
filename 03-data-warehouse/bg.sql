-- setup
CREATE OR REPLACE EXTERNAL TABLE `dw_dataset.green_taxi_data`
(
    VendorID INT64,
    passenger_count INT64,
    trip_distance FLOAT64,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    RatecodeID INT64,
    store_and_fwd_flag STRING,
    PULocationID INT64,
    DOLocationID INT64,
    payment_type INT64,
    fare_amount FLOAT64,
    extra FLOAT64,
    mta_tax FLOAT64,
    tip_amount FLOAT64,
    tolls_amount FLOAT64,
    improvement_surcharge FLOAT64,
    total_amount FLOAT64,
    congestion_surcharge FLOAT64
)
OPTIONS (
  format = 'PARQUET',  
  uris = ['gs://dw_taxi_data/green_taxi_data_partitioned/*']
);


CREATE OR REPLACE TABLE `dw_dataset.green_taxi_data_nonpartitioned`
AS SELECT * FROM `dw_dataset.green_taxi_data`;

CREATE OR REPLACE TABLE `dw_dataset.green_taxi_data_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `dw_dataset.green_taxi_data`
);

--test
SELECT * from `dw_dataset.green_taxi_data` limit 10;
SELECT * from `dw_dataset.green_taxi_data_nonpartitioned` limit 10;
SELECT * from `dw_dataset.green_taxi_data_partitioned_clustered` limit 10;

SELECT count(*) from `dw_dataset.green_taxi_data` limit 10;
SELECT count(*) from `dw_dataset.green_taxi_data_nonpartitioned` limit 10;
SELECT count(*) from `dw_dataset.green_taxi_data_partitioned_clustered` limit 10;


-- 1 
SELECT count(*) FROM `dw_dataset.green_taxi_data`;

-- 2
SELECT COUNT(DISTINCT(PULocationID)) FROM `dw_dataset.green_taxi_data`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `dw_dataset.green_taxi_data_nonpartitioned`;

-- 3
SELECT COUNT(1) FROM `dw_dataset.green_taxi_data_nonpartitioned` WHERE fare_amount = 0

-- 4
CREATE OR REPLACE TABLE `dw_dataset.green_taxi_data_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `dw_dataset.green_taxi_data`
);

-- 5
SELECT DISTINCT PULocationID FROM `dw_dataset.green_taxi_data_nonpartitioned`
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP('2022-06-01') AND TIMESTAMP('2022-06-30');

SELECT DISTINCT PULocationID FROM `dw_dataset.green_taxi_data_partitioned_clustered`
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP('2022-06-01') AND TIMESTAMP('2022-06-30');

-- 8
SELECT count(*) FROM `dw_dataset.green_taxi_data_nonpartitioned` 