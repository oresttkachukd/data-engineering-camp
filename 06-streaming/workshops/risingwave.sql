---- 0	
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;


---- 1
CREATE MATERIALIZED VIEW trip_time_statistics AS
	SELECT
		PULocationID AS pickup_location,
		DOLocationID AS dropoff_location,
		AVG(EXTRACT(EPOCH FROM tpep_dropoff_datetime - tpep_pickup_datetime)) AS average_trip_time,
		MIN(EXTRACT(EPOCH FROM tpep_dropoff_datetime - tpep_pickup_datetime)) AS min_trip_time,
		MAX(EXTRACT(EPOCH FROM tpep_dropoff_datetime - tpep_pickup_datetime)) AS max_trip_time
	FROM
		trip_data
	GROUP BY
		PULocationID,
		DOLocationID;

select taxi_zone_pickup.Zone as taxi_zone_pickup, taxi_zone_dropoff.Zone as taxi_zone_dropoff, trip_time_statistics.* from trip_time_statistics 
JOIN taxi_zone as taxi_zone_pickup ON trip_time_statistics.pickup_location = taxi_zone_pickup.location_id	
JOIN taxi_zone as taxi_zone_dropoff  ON trip_time_statistics.dropoff_location = taxi_zone_dropoff.location_id	
order by average_trip_time desc;


--2
CREATE MATERIALIZED VIEW trip_time_statistics_modified AS
 SELECT
     PULocationID AS pickup_location,
 	 taxi_zone_pickup.Zone as pickup_taxi_zone, 
     DOLocationID AS dropoff_location,
 	 taxi_zone_dropoff.Zone as dropoff_taxi_zone,
     AVG(EXTRACT(EPOCH FROM tpep_dropoff_datetime - tpep_pickup_datetime)) AS average_trip_time,
     MIN(EXTRACT(EPOCH FROM tpep_dropoff_datetime - tpep_pickup_datetime)) AS min_trip_time,
     MAX(EXTRACT(EPOCH FROM tpep_dropoff_datetime - tpep_pickup_datetime)) AS max_trip_time,
     COUNT(*) AS trip_count
 FROM
     trip_data
 JOIN taxi_zone as taxi_zone_pickup ON trip_data.PULocationID = taxi_zone_pickup.location_id	
 JOIN taxi_zone as taxi_zone_dropoff  ON trip_data.DOLocationID = taxi_zone_dropoff.location_id		
 GROUP BY
     PULocationID,
	 taxi_zone_pickup.Zone,
     DOLocationID,
	 taxi_zone_dropoff.Zone;



 ORDER BY
     average_trip_time DESC
 LIMIT 1;	
 
 
--- 3
WITH dynamic_filter AS (
    SELECT
        MAX(tpep_pickup_datetime) AS latest_pickup_time
    FROM
        trip_data
)
	SELECT
		PULocationID AS pickup_location,
		taxi_zone_pickup.Zone as pickup_taxi_zone,
		COUNT(*) AS pickup_count
	FROM
		trip_data
	JOIN taxi_zone as taxi_zone_pickup ON trip_data.PULocationID = taxi_zone_pickup.location_id		
	WHERE
		tpep_pickup_datetime >= (SELECT latest_pickup_time - INTERVAL '17 hours' FROM dynamic_filter)
		AND tpep_pickup_datetime <= (SELECT latest_pickup_time FROM dynamic_filter)
	GROUP BY PULocationID, taxi_zone_pickup.Zone
	ORDER BY pickup_count DESC
	LIMIT 3;