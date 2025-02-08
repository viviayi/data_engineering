-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-448111.zoomcamp.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc-de-course-448111-kestra/yellow_tripdata_2024-*.parquet']
);

-- Creating a table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).
CREATE OR REPLACE TABLE dtc-de-course-448111.zoomcamp.yellow_tripdata_non_partitoned_2024 AS
SELECT * FROM dtc-de-course-448111.zoomcamp.external_yellow_tripdata_2024;

-- query to count the distinct number of PULocationIDs
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocation_count
FROM `dtc-de-course-448111.zoomcamp.external_yellow_tripdata_2024`;

SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocation_count
FROM `dtc-de-course-448111.zoomcamp.yellow_tripdata_non_partitoned_2024`;

-- Question 3
SELECT PULocationID
FROM `dtc-de-course-448111.zoomcamp.yellow_tripdata_non_partitoned_2024`;

SELECT PULocationID, DOLocationID
FROM `dtc-de-course-448111.zoomcamp.yellow_tripdata_non_partitoned_2024`;

-- Question 4
SELECT COUNT(*) AS zero_fare_count
FROM `dtc-de-course-448111.zoomcamp.yellow_tripdata_non_partitoned_2024`
WHERE fare_amount = 0;


-- Question5 Partition by tpep_dropoff_datetime and Cluster on VendorID
CREATE OR REPLACE TABLE `dtc-de-course-448111.zoomcamp.optimized_yellow_taxi_trip_records`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `dtc-de-course-448111.zoomcamp.yellow_tripdata_non_partitoned_2024`;

-- Question 6
SELECT DISTINCT VendorID
FROM `dtc-de-course-448111.zoomcamp.yellow_tripdata_non_partitoned_2024`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `dtc-de-course-448111.zoomcamp.optimized_yellow_taxi_trip_records`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';