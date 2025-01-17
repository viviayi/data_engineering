Question 3. Trip Segmentation Count
```sql
SELECT 
    DATE(lpep_pickup_datetime) AS pickup_date,
    MAX(trip_distance) AS longest_trip_distance
FROM 
    trips
GROUP BY 
    DATE(lpep_pickup_datetime)
ORDER BY 
    longest_trip_distance DESC
LIMIT 1;
```

Question 4. Longest trip for each day
```sql
SELECT 
    DATE(lpep_pickup_datetime) AS pickup_date,
    MAX(trip_distance) AS longest_trip_distance
FROM 
    green_trip_data
GROUP BY 
    DATE(lpep_pickup_datetime)
ORDER BY 
    longest_trip_distance DESC
LIMIT 1;
```

Question 5. Three biggest pickup zones
```sql
SELECT 
    z."Zone" AS pickup_zone,
    SUM(t.total_amount) AS total_amount
FROM 
    green_trip_data t
JOIN 
    zone z
ON 
    t."PULocationID" = z."LocationID"
WHERE 
    DATE(t.lpep_pickup_datetime) = '2019-10-18'
GROUP BY 
    z."Zone"
HAVING 
    SUM(t.total_amount) > 13000
ORDER BY 
    total_amount DESC;
```

Question 6. Largest tip

```sql
SELECT
	z_drop."Zone" AS dropoff_zone,
	MAX(t.tip_amount) AS largest_tip
FROM
	green_trip_data t
JOIN 
    zone z_pickup ON t."PULocationID" = z_pickup."LocationID"
JOIN 
    zone z_drop ON t."DOLocationID" = z_drop."LocationID"
WHERE 
    z_pickup."Zone" = 'East Harlem North'
    AND DATE(t.lpep_pickup_datetime) BETWEEN '2019-10-01' AND '2019-10-31'
GROUP BY 
    z_drop."Zone"
ORDER BY 
    largest_tip DESC
LIMIT 1;
```
