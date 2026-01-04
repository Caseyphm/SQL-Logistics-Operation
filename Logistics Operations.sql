USE Logistics;

DROP VIEW IF EXISTS events_agg;

CREATE VIEW events_agg AS
SELECT trip_id,
		SUM(on_time_flag + 0) AS on_time_flag,
		COUNT(on_time_flag) AS total_flag
FROM delivery_events
GROUP BY trip_id;

SELECT * FROM events_agg;

-- 1. On-time Analysis
-- 1.1. Company level

WITH company_on_time AS 
	(SELECT ti.dispatch_date,
			DATEFROMPARTS(YEAR(ti.dispatch_date), MONTH(ti.dispatch_date), 1) AS dispatch_month,
			COUNT(ti.trip_id) AS trip_completed,
			SUM(ev.on_time_flag) AS on_time_flag,
			SUM(ev.total_flag) AS total_flag
	FROM trips AS ti
	FULL JOIN events_agg AS ev
	ON ti.trip_id = ev.trip_id
	GROUP BY ti.dispatch_date)

SELECT  dispatch_month,
		SUM(trip_completed) AS monthly_trip_completed,
		SUM(on_time_flag) AS monthly_on_time_flag,
		SUM(total_flag) AS monthly_total_flag,
		100.0 * SUM(on_time_flag)/SUM(total_flag) AS monthly_on_time_rate,
		DATEFROMPARTS(YEAR(dispatch_month), 1, 1) AS dispatch_year,
		SUM(SUM(trip_completed)) OVER(PARTITION BY YEAR(dispatch_month)) AS yearly_trip_completed,
		SUM(SUM(on_time_flag)) OVER(PARTITION BY YEAR(dispatch_month)) AS yearly_on_time_flag,
		SUM(SUM(total_flag)) OVER(PARTITION BY YEAR(dispatch_month)) AS yearly_total_flag,
		100.0 * SUM(SUM(on_time_flag)) OVER(PARTITION BY YEAR(dispatch_month))/
		SUM(SUM(total_flag)) OVER(PARTITION BY YEAR(dispatch_month)) AS yearly_on_time_rate
FROM company_on_time
GROUP BY dispatch_month
ORDER BY dispatch_month;

-- 1.2. Driver level
WITH driver_on_time AS
	(SELECT ti.driver_id, ti.dispatch_date,
			DATEFROMPARTS(YEAR(dispatch_date), MONTH(dispatch_date), 1) AS dispatch_month,
			COUNT(ti.trip_id) AS trip_completed,
			SUM(ev.on_time_flag) AS on_time_flag,
			SUM(ev.total_flag) AS total_flag
	FROM trips AS ti
	FULL JOIN events_agg AS ev
	ON ti.trip_id = ev.trip_id
	WHERE ti.driver_id IS NOT NULL
	GROUP BY ti.dispatch_date, ti.driver_id)

SELECT  driver_id, dispatch_month,
		SUM(trip_completed) AS monthly_trip_completed,
		SUM(on_time_flag) AS monthly_on_time_flag,
		SUM(total_flag) AS monthly_total_flag,
		100.0 * SUM(on_time_flag)/SUM(total_flag) AS monthly_on_time_rate,
		DATEFROMPARTS(YEAR(dispatch_month), 1, 1) AS dispatch_year,
		SUM(SUM(trip_completed)) OVER(PARTITION BY driver_id, YEAR(dispatch_month)) AS yearly_trip_completed,
		SUM(SUM(on_time_flag)) OVER(PARTITION BY driver_id, YEAR(dispatch_month)) AS yearly_on_time_flag,
		SUM(SUM(total_flag)) OVER(PARTITION BY driver_id, YEAR(dispatch_month)) AS yearly_total_flag,
		100.0 * SUM(SUM(on_time_flag)) OVER(PARTITION BY driver_id, YEAR(dispatch_month))/
		SUM(SUM(total_flag)) OVER(PARTITION BY driver_id, YEAR(dispatch_month)) AS yearly_on_time_rate
FROM driver_on_time
GROUP BY driver_id, dispatch_month
ORDER BY driver_id, dispatch_month;

WITH driver_on_time AS
	(SELECT ti.driver_id, ti.dispatch_date,
			DATEFROMPARTS(YEAR(dispatch_date), MONTH(dispatch_date), 1) AS dispatch_month,
			COUNT(ti.trip_id) AS trip_completed,
			SUM(ev.on_time_flag) AS on_time_flag,
			SUM(ev.total_flag) AS total_flag
	FROM trips AS ti
	FULL JOIN events_agg AS ev
	ON ti.trip_id = ev.trip_id
	WHERE ti.driver_id IS NOT NULL
	GROUP BY ti.dispatch_date, ti.driver_id),

driver_rank_on_time_rate AS
	(SELECT  driver_id, dispatch_month,
			SUM(trip_completed) AS monthly_trip_completed,
			SUM(on_time_flag) AS monthly_on_time_flag,
			SUM(total_flag) AS monthly_total_flag,
			100.0 * SUM(on_time_flag)/SUM(total_flag) AS monthly_on_time_rate,
			RANK() OVER(PARTITION BY dispatch_month ORDER BY 100.0 * SUM(on_time_flag)/SUM(total_flag)) AS rank_on_time_rate
	FROM driver_on_time
	GROUP BY driver_id, dispatch_month)

SELECT  driver_rank_on_time_rate.driver_id,
		COUNT(*) AS in_bottom_5,
		drivers.years_experience
FROM driver_rank_on_time_rate
LEFT JOIN drivers
ON driver_rank_on_time_rate.driver_id = drivers.driver_id
WHERE rank_on_time_rate <= 5
GROUP BY driver_rank_on_time_rate.driver_id, drivers.years_experience
ORDER BY in_bottom_5 DESC;

-- 2. Time Utilization Analysis
-- 2.1. Company level
WITH company_time_utilization AS
	(SELECT dispatch_date,
			DATEFROMPARTS(YEAR(dispatch_date), MONTH(dispatch_date), 1) AS dispatch_month,
			COUNT(trip_id) AS trip_completed,
			SUM(actual_duration_hours) AS actual_duration_hours,
			SUM(idle_time_hours) AS idle_time_hours
	FROM trips
	GROUP BY dispatch_date)

SELECT  dispatch_month,
		SUM(trip_completed) AS monthly_trip_completed,
		SUM(actual_duration_hours) AS monthly_actual_hours,
		SUM(idle_time_hours) AS monthly_idle_hours,
		SUM(actual_duration_hours)/(SUM(actual_duration_hours) + SUM(idle_time_hours)) AS monthly_time_utilization,
		DATEFROMPARTS(YEAR(dispatch_month), 1, 1) AS dispatch_year,
		SUM(SUM(actual_duration_hours)) OVER(PARTITION BY YEAR(dispatch_month)) AS yearly_actual_hours,
		SUM(SUM(idle_time_hours)) OVER(PARTITION BY YEAR(dispatch_month)) AS yearly_idle_hours,
		SUM(SUM(actual_duration_hours)) OVER(PARTITION BY YEAR(dispatch_month))/
		(SUM(SUM(actual_duration_hours)) OVER(PARTITION BY YEAR(dispatch_month)) +
		SUM(SUM(idle_time_hours)) OVER(PARTITION BY YEAR(dispatch_month))) AS yearly_time_utilization
FROM company_time_utilization
GROUP BY dispatch_month
ORDER BY dispatch_month;

-- 2.2. Driver level
WITH driver_time_utilization AS
	(SELECT driver_id,
			dispatch_date,
			DATEFROMPARTS(YEAR(dispatch_date), MONTH(dispatch_date), 1) AS dispatch_month,
			COUNT(trip_id) AS trip_completed,
			SUM(actual_duration_hours) AS actual_duration_hours,
			SUM(idle_time_hours) AS idle_time_hours
	FROM trips
	WHERE driver_id IS NOT NULL
	GROUP BY driver_id, dispatch_date)

SELECT  driver_id, dispatch_month,
		SUM(trip_completed) AS monthly_trip_completed,
		SUM(actual_duration_hours) AS monthly_actual_duration_hours,
		SUM(idle_time_hours) AS monthly_idle_time_hours,
		SUM(actual_duration_hours)/(SUM(actual_duration_hours) + SUM(idle_time_hours)) AS monthly_time_utilization,
		DATEFROMPARTS(YEAR(dispatch_month),1,1) AS dispatch_year,
		SUM(SUM(actual_duration_hours)) OVER(PARTITION BY driver_id, YEAR(dispatch_month)) AS yearly_actual_duration_hours,
		SUM(SUM(idle_time_hours)) OVER(PARTITION BY driver_id, YEAR(dispatch_month)) AS yearly_idle_time_hours,
		SUM(SUM(actual_duration_hours)) OVER(PARTITION BY driver_id, YEAR(dispatch_month))/
		(SUM(SUM(actual_duration_hours)) OVER(PARTITION BY driver_id, YEAR(dispatch_month))+
		SUM(SUM(idle_time_hours)) OVER(PARTITION BY driver_id, YEAR(dispatch_month))) AS yearly_time_utilization
FROM driver_time_utilization
GROUP BY driver_id, dispatch_month
ORDER BY driver_id, dispatch_month;

WITH driver_time_utilization AS
	(SELECT driver_id,
			dispatch_date,
			DATEFROMPARTS(YEAR(dispatch_date), MONTH(dispatch_date), 1) AS dispatch_month,
			COUNT(trip_id) AS trip_completed,
			SUM(actual_duration_hours) AS actual_duration_hours,
			SUM(idle_time_hours) AS idle_time_hours
	FROM trips
	WHERE driver_id IS NOT NULL
	GROUP BY driver_id, dispatch_date),

driver_rank_time_utilization AS
	(SELECT driver_id, dispatch_month,
		SUM(trip_completed) AS monthly_trip_completed,
		SUM(actual_duration_hours) AS monthly_actual_duration_hours,
		SUM(idle_time_hours) AS monthly_idle_time_hours,
		SUM(actual_duration_hours)/(SUM(actual_duration_hours) + SUM(idle_time_hours)) AS monthly_time_utilization,
		RANK() OVER(PARTITION BY dispatch_month ORDER BY
		SUM(actual_duration_hours)/(SUM(actual_duration_hours) + SUM(idle_time_hours))) AS rank_time_utilization
	FROM driver_time_utilization
	GROUP BY driver_id, dispatch_month)

SELECT  driver_rank_time_utilization.driver_id,
		COUNT(*) AS in_bottom_5,
		DATEDIFF(YEAR,drivers.hire_date,'2024-12-31') AS years_working
FROM driver_rank_time_utilization
LEFT JOIN drivers
ON driver_rank_time_utilization.driver_id = drivers.driver_id
WHERE rank_time_utilization <= 5
GROUP BY driver_rank_time_utilization.driver_id, hire_date
ORDER BY in_bottom_5 DESC;

-- 3. Safety Analysis
-- 3.1. Company level
WITH company_safety AS
	(SELECT ti.trip_id, ti.dispatch_date,
			DATEFROMPARTS(YEAR(ti.dispatch_date), MONTH(ti.dispatch_date), 1) AS dispatch_month,
			sa.incident_id, sa.incident_date, sa.location_city,
			sa.at_fault_flag, sa.preventable_flag, sa.description,
			sa.vehicle_damage_cost, sa.cargo_damage_cost, sa.claim_amount
	FROM trips AS ti
	FULL JOIN safety_incidents AS sa
	ON ti.trip_id = sa.trip_id)

SELECT dispatch_month,
		COUNT(incident_id) AS monthly_number_of_incident,
		COUNT(trip_id) AS monthly_trip_completed,
		100.0 * COUNT(incident_id) / COUNT(trip_id) AS monthly_incident_rate,
		DATEFROMPARTS(YEAR(dispatch_month),1,1) AS dispatch_year,
		SUM(COUNT(incident_id)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_number_of_incident,
		SUM(COUNT(trip_id)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_trip_completed,
		100.0 * SUM(COUNT(incident_id)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) /
		SUM(COUNT(trip_id)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_incident_rate
FROM company_safety
GROUP BY dispatch_month
ORDER BY dispatch_month;

WITH company_safety AS
	(SELECT ti.trip_id, ti.dispatch_date,
			DATEFROMPARTS(YEAR(ti.dispatch_date), MONTH(ti.dispatch_date), 1) AS dispatch_month,
			sa.incident_id, sa.incident_date, sa.location_city,
			sa.at_fault_flag, sa.preventable_flag, sa.description,
			sa.vehicle_damage_cost, sa.cargo_damage_cost, sa.claim_amount
	FROM trips AS ti
	FULL JOIN safety_incidents AS sa
	ON ti.trip_id = sa.trip_id)

SELECT  dispatch_month,
		SUM(at_fault_flag+0) AS monthly_at_fault_flag,
		SUM(preventable_flag+0) AS monthly_preventable_flag,
		COUNT(incident_id) AS monthly_number_of_incident,
		100.0 * SUM(at_fault_flag+0) / COUNT(incident_id) AS monthly_at_fault_pct,
		100.0 * SUM(preventable_flag+0) / COUNT(incident_id) AS monthly_preventable_pct,
		DATEFROMPARTS(YEAR(dispatch_month),1,1) AS dispatch_year,
		SUM(SUM(at_fault_flag+0)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_at_fault_flag,
		SUM(SUM(preventable_flag+0)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_preventable_flag,
		SUM(COUNT(incident_id)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_number_of_incident,
		100.0 * SUM(SUM(at_fault_flag+0)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) /
		SUM(COUNT(incident_id)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_at_fault_pct,
		100.0 * SUM(SUM(preventable_flag+0)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) /
		SUM(COUNT(incident_id)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_preventable_pct
FROM company_safety
GROUP BY dispatch_month
ORDER BY dispatch_month;

WITH company_safety AS
	(SELECT ti.trip_id, ti.dispatch_date,
			DATEFROMPARTS(YEAR(ti.dispatch_date), MONTH(ti.dispatch_date), 1) AS dispatch_month,
			sa.incident_id, sa.incident_date, sa.location_city,
			sa.at_fault_flag, sa.preventable_flag, sa.description,
			sa.vehicle_damage_cost, sa.cargo_damage_cost, sa.claim_amount
	FROM trips AS ti
	FULL JOIN safety_incidents AS sa
	ON ti.trip_id = sa.trip_id)

SELECT  location_city,
		COUNT(incident_id) AS monthly_number_of_incident
FROM company_safety
GROUP BY location_city
HAVING location_city IS NOT NULL
ORDER BY monthly_number_of_incident DESC;

-- 3.2. Driver level
WITH driver_safety AS
	(SELECT ti.trip_id, ti.driver_id, ti.dispatch_date,
			DATEFROMPARTS(YEAR(ti.dispatch_date), MONTH(ti.dispatch_date), 1) AS dispatch_month,
			sa.incident_id, sa.incident_date, sa.location_city,
			sa.at_fault_flag, sa.preventable_flag, sa.description,
			sa.vehicle_damage_cost, sa.cargo_damage_cost, sa.claim_amount
	FROM trips AS ti
	FULL JOIN safety_incidents AS sa
	ON ti.trip_id = sa.trip_id)

SELECT  driver_id, dispatch_month,
		COUNT(incident_id) AS monthly_number_of_incident,
		COUNT(trip_id) AS monthly_trip_completed,
		100.0 * COUNT(incident_id) / COUNT(trip_id) AS monthly_incident_rate,
		DATEFROMPARTS(YEAR(dispatch_month),1,1) AS dispatch_year,
		SUM(COUNT(incident_id)) OVER(PARTITION BY driver_id, DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_number_of_incident,
		SUM(COUNT(trip_id)) OVER(PARTITION BY driver_id, DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_trip_completed,
		100.0 * SUM(COUNT(incident_id)) OVER(PARTITION BY driver_id, DATEFROMPARTS(YEAR(dispatch_month),1,1)) /
		SUM(COUNT(trip_id)) OVER(PARTITION BY driver_id, DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_incident_rate
FROM driver_safety
GROUP BY driver_id, dispatch_month
HAVING driver_id IS NOT NULL
ORDER BY driver_id, dispatch_month;

WITH driver_safety AS
	(SELECT ti.trip_id, ti.driver_id, ti.dispatch_date,
			DATEFROMPARTS(YEAR(ti.dispatch_date), MONTH(ti.dispatch_date), 1) AS dispatch_month,
			sa.incident_id, sa.incident_date, sa.location_city,
			sa.at_fault_flag, sa.preventable_flag, sa.description,
			sa.vehicle_damage_cost, sa.cargo_damage_cost, sa.claim_amount
	FROM trips AS ti
	FULL JOIN safety_incidents AS sa
	ON ti.trip_id = sa.trip_id)
	
SELECT  driver_id,
		COUNT(trip_id) AS trip_completed,
		COUNT(incident_id) AS number_of_incident,
		SUM(at_fault_flag+0) AS at_fault_flag,
		SUM(preventable_flag+0) AS preventable_flag,
		100.0 * SUM(at_fault_flag+0) / COUNT(incident_id) AS at_fault_pct,
		100.0 * SUM(preventable_flag+0) / COUNT(incident_id) AS preventable_pct
FROM driver_safety
GROUP BY driver_id
HAVING driver_id IS NOT NULL
ORDER BY number_of_incident DESC, at_fault_flag DESC;

-- 4. Miles Per Gallon (MPG) Analysis
-- 4.1. Company level
WITH company_mpg AS
	(SELECT trip_id, dispatch_date,
			DATEFROMPARTS(YEAR(dispatch_date), MONTH(dispatch_date), 1) AS dispatch_month,
			actual_distance_miles, fuel_gallons_used
	FROM trips)

SELECT  dispatch_month,
		COUNT(trip_id) AS monthly_trip_completed,
		SUM(actual_distance_miles) AS monthly_miles,
		SUM(fuel_gallons_used) AS monthly_gallons,
		SUM(actual_distance_miles) / SUM(fuel_gallons_used) AS monthly_mpg,
		DATEFROMPARTS(YEAR(dispatch_month),1,1) AS dispatch_year,
		SUM(COUNT(trip_id)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_trip_completed,
		SUM(SUM(actual_distance_miles)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_miles,
		SUM(SUM(fuel_gallons_used)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_gallons,
		SUM(SUM(actual_distance_miles)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) / 
		SUM(SUM(fuel_gallons_used)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) year_mpg
FROM company_mpg
GROUP BY dispatch_month
ORDER BY dispatch_month;

-- 4.2. Truck level
WITH driver_mpg AS
	(SELECT trip_id, truck_id, dispatch_date,
			DATEFROMPARTS(YEAR(dispatch_date), MONTH(dispatch_date), 1) AS dispatch_month,
			actual_distance_miles, fuel_gallons_used
	FROM trips)

SELECT  truck_id, dispatch_month,
		COUNT(trip_id) AS monthly_trip_completed,
		SUM(actual_distance_miles) AS monthly_miles,
		SUM(fuel_gallons_used) AS monthly_gallons,
		SUM(actual_distance_miles) / SUM(fuel_gallons_used) AS monthly_mpg,
		DATEFROMPARTS(YEAR(dispatch_month),1,1) AS dispatch_year,
		SUM(COUNT(trip_id)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_trip_completed,
		SUM(SUM(actual_distance_miles)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_miles,
		SUM(SUM(fuel_gallons_used)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) AS yearly_gallons,
		SUM(SUM(actual_distance_miles)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) / 
		SUM(SUM(fuel_gallons_used)) OVER(PARTITION BY DATEFROMPARTS(YEAR(dispatch_month),1,1)) year_mpg
FROM driver_mpg
GROUP BY truck_id, dispatch_month
HAVING truck_id IS NOT NULL
ORDER BY truck_id, dispatch_month;

WITH driver_mpg AS
	(SELECT trip_id, truck_id, dispatch_date,
			DATEFROMPARTS(YEAR(dispatch_date), MONTH(dispatch_date), 1) AS dispatch_month,
			actual_distance_miles, fuel_gallons_used
	FROM trips),
truck_rank_mpg AS
	(SELECT  truck_id, dispatch_month,
			COUNT(trip_id) AS monthly_trip_completed,
			SUM(actual_distance_miles) AS monthly_miles,
			SUM(fuel_gallons_used) AS monthly_gallons,
			SUM(actual_distance_miles) / SUM(fuel_gallons_used) AS monthly_mpg,
			RANK() OVER(PARTITION BY dispatch_month ORDER BY SUM(actual_distance_miles) / SUM(fuel_gallons_used)) AS rank_mpg
	FROM driver_mpg
	GROUP BY truck_id, dispatch_month
	HAVING truck_id IS NOT NULL)

SELECT  truck_id,
		COUNT(*) AS in_bottom_5
FROM truck_rank_mpg
WHERE rank_mpg <= 5
GROUP BY truck_id
ORDER BY in_bottom_5 DESC;