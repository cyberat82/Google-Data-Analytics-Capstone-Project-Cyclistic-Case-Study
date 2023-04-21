  -- Create a new table with all the trip data for 2022
CREATE TABLE
  courseprojects-382909.bike_project.combined_tripdata AS
SELECT
  *
FROM (
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202201-divvy-tripdata -- Add all data from January 2022
  UNION ALL
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202202-divvy-tripdata -- Add all data from February 2022
  UNION ALL
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202203-divvy-tripdata -- Add all data from March 2022
  UNION ALL
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202204-divvy-tripdata -- Add all data from April 2022
  UNION ALL
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202205-divvy-tripdata -- Add all data from May 2022
  UNION ALL
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202206-divvy-tripdata -- Add all data from June 2022
  UNION ALL
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202207-divvy-tripdata -- Add all data from July 2022
  UNION ALL
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202208-divvy-tripdata -- Add all data from August 2022
  UNION ALL
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202209-divvy-tripdata -- Add all data from September 2022
  UNION ALL
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202210-divvy-tripdata -- Add all data from October 2022
  UNION ALL
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202211-divvy-tripdata -- Add all data from November 2022
  UNION ALL
  SELECT
    *
  FROM
    courseprojects-382909.bike_project.202212-divvy-tripdata -- Add all data from December 2022
    )
ORDER BY
  started_at; -- Order the data by the trip start time

  -- Count the number of ride_ids with different lengths
SELECT
  LENGTH(ride_id),
  COUNT(*)
FROM
  courseprojects-382909.bike_project.combined_tripdata
GROUP BY
  LENGTH(ride_id);

  -- Count the total number of ride_ids
SELECT
  COUNT(DISTINCT ride_id)
FROM
  courseprojects-382909.bike_project.combined_tripdata;

  -- Find all the unique rideable types
SELECT
  DISTINCT rideable_type
FROM
  courseprojects-382909.bike_project.combined_tripdata;

  -- Count the number of trips that lasted between 1 and 1440 minutes (inclusive)
SELECT
  COUNT(*)
FROM
  courseprojects-382909.bike_project.combined_tripdata
WHERE
  TIMESTAMP_DIFF(ended_at, started_at, MINUTE) >= 1
  OR TIMESTAMP_DIFF(ended_at, started_at, MINUTE) <= 1440;

  -- Count the number of trips with a missing start station name
SELECT
  COUNT(*)
FROM
  courseprojects-382909.bike_project.combined_tripdata
WHERE
  start_station_name IS NULL;

  -- Find all the trips with a start station name that has leading/trailing spaces
SELECT
  *
FROM
  `courseprojects-382909.bike_project.combined_tripdata`
WHERE
  start_station_name LIKE ' %'
  OR start_station_name LIKE '% ' OR;

  -- Counts the number of rows where either start_lat or start_lng is NULL
SELECT
  COUNT(*)
FROM
  courseprojects-382909.bike_project.combined_tripdata
WHERE
  start_lat IS NULL
  OR start_lng IS NULL;

  -- Counts the number of trips made by members and casual users separately
SELECT
  COUNT(CASE
      WHEN member_casual LIKE "%member%" THEN 1
  END
    ) AS members,
  COUNT(CASE
      WHEN member_casual LIKE "%casual%" THEN 1
  END
    ) AS casual
FROM
  courseprojects-382909.bike_project.combined_tripdata;

  -- DATA CLEANING: 
  -- Creates a cleaned dataset with selected columns, filters the trips with length between 1 and 1440 minutes, and orders by month, day of week, and time
WITH
  cleaned_dataset AS (
  SELECT
    *
  FROM (
    SELECT
      rideable_type,
      FORMAT_DATETIME('%B', started_at) AS month,
      FORMAT_DATETIME('%A', started_at) AS day_of_week,
      TIME(started_at) AS time,
      start_station_name,
      start_lat,
      start_lng,
      member_casual,
      TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS trip_length
    FROM
      courseprojects-382909.bike_project.combined_tripdata
    WHERE
      TIMESTAMP_DIFF(ended_at, started_at, MINUTE) >= 1
      AND TIMESTAMP_DIFF(ended_at, started_at, MINUTE) <= 1440
    ORDER BY
      CASE FORMAT_DATETIME('%B', started_at)
        WHEN 'January' THEN 1
        WHEN 'February' THEN 2
        WHEN 'March' THEN 3
        WHEN 'April' THEN 4
        WHEN 'May' THEN 5
        WHEN 'June' THEN 6
        WHEN 'July' THEN 7
        WHEN 'August' THEN 8
        WHEN 'September' THEN 9
        WHEN 'October' THEN 10
        WHEN 'November' THEN 11
        WHEN 'December' THEN 12
    END
      ,
      CASE FORMAT_DATETIME('%A', started_at)
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END
      ,
      time ) ),
  -- Define trips_per_hour subquery to count the number of trips per hour
trips_per_hour AS (
SELECT
EXTRACT(HOUR FROM time) AS hour,
COUNT(IF(member_casual = 'casual', 1, NULL)) AS num_trips_casual,
COUNT(IF(member_casual = 'member', 1, NULL)) AS num_trips_member
FROM
cleaned_dataset
GROUP BY
hour
ORDER BY
hour
),
-- Define trips_per_day subquery to count the number of trips per day of the week
trips_per_day AS (
SELECT
day_of_week,
COUNTIF(member_casual = 'casual') AS num_trips_casual,
COUNTIF(member_casual = 'member') AS num_trips_member
FROM
cleaned_dataset
GROUP BY
day_of_week
ORDER BY
CASE day_of_week
WHEN 'Monday' THEN 1
WHEN 'Tuesday' THEN 2
WHEN 'Wednesday' THEN 3
WHEN 'Thursday' THEN 4
WHEN 'Friday' THEN 5
WHEN 'Saturday' THEN 6
WHEN 'Sunday' THEN 7
END
),
-- Define trips_per_month subquery to count the number of trips per month
trips_per_month AS (
SELECT
month,
COUNT(IF(member_casual = 'casual', 1, NULL)) AS casual_trips,
COUNT(IF(member_casual = 'member', 1, NULL)) AS member_trips
FROM
cleaned_dataset
GROUP BY
month
ORDER BY
CASE
WHEN month = 'January' THEN 1
WHEN month = 'February' THEN 2
WHEN month = 'March' THEN 3
WHEN month = 'April' THEN 4
WHEN month = 'May' THEN 5
WHEN month = 'June' THEN 6
WHEN month = 'July' THEN 7
WHEN month = 'August' THEN 8
WHEN month = 'September' THEN 9
WHEN month = 'October' THEN 10
WHEN month = 'November' THEN 11
WHEN month = 'December' THEN 12
END
),
-- Define top_10_stations subquery to find the top 10 stations with the most trips
top_10_stations AS (
SELECT
start_station_name,
start_lat,
start_lng,
COUNT(*) AS trips_number,
COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) AS casual_trips,
COUNT(CASE WHEN member_casual = 'member' THEN 1 END) AS member_trips
FROM
cleaned_dataset
GROUP BY
start_station_name,
start_lat,
start_lng
ORDER BY
trips_number DESC
LIMIT
10
),
-- Define trips_per_bike_type subquery to count the number of trips per bike type
trips_per_bike_type AS (
SELECT
rideable_type,
COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) AS casual_trips,
COUNT(CASE WHEN member_casual = 'member' THEN 1 END) AS member_trips
FROM
cleaned_dataset
GROUP BY
rideable_type
ORDER BY
rideable_type
),
  -- Calculate the average trip length for each member type
avg_trip_length AS (
SELECT
member_casual,
AVG(trip_length) AS avg_trip_length
FROM
cleaned_dataset
GROUP BY
member_casual
),

-- Calculate the average trip length per hour for each member type
avg_per_hour_trip_length AS (
SELECT
member_casual,
EXTRACT(HOUR FROM time) AS hour,
ROUND(AVG(trip_length)) AS avg_per_hour_trip_length
FROM
cleaned_dataset
GROUP BY
member_casual,
hour
ORDER BY
member_casual,
hour
),

-- Calculate the average trip length per day of the week for each member type
avg_per_day_trip_length AS (
SELECT
member_casual,
day_of_week,
ROUND(AVG(trip_length)) AS avg_per_day_trip_length
FROM
cleaned_dataset
GROUP BY
member_casual,
day_of_week
ORDER BY
member_casual,
CASE day_of_week
WHEN 'Monday' THEN 1
WHEN 'Tuesday' THEN 2
WHEN 'Wednesday' THEN 3
WHEN 'Thursday' THEN 4
WHEN 'Friday' THEN 5
WHEN 'Saturday' THEN 6
WHEN 'Sunday' THEN 7
END
)
