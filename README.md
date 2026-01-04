# SQL-Logistics-Operation
#Project Summary
To continue developing supply-chain analytics skills, I analyzed three years (2022–2024) of operational performance for a fictional logistics company using the Logistics Operations Database (Yogape Rodriguez). The analysis measures on-time delivery, time utilization, safety (accidents and causation), and fleet fuel efficiency (MPG). Results are produced at monthly and annual granularity and segmented by company scale and driver cohorts.

Dataset: [Kaggle — Logistics Operations Database](https://www.kaggle.com/datasets/yogape/logistics-operations-database)

#Objectives & KPIs
Primary goal: Assess operational performance of drivers and trucks to inform workforce management, safety programs and fleet maintenance planning.

Key KPIs:

On-time performance — On-time performance = on_time_flag / total_flag
Time Utilization — Time Utilization = working_hours / (working_hours + idle_hours)
Accident rate and breakdown (at-fault incidents, preventable incidents)
Fuel efficiency (MPG) — MPG = miles / gallons

#Key Results
Company monthly On-time performance: 54%–56% (annual approximately 55.5%) — below typical benchmarks.
Driver-level On-time performance volatile; two drivers appear 5 times in monthly bottom-5 despite 9 and 11 years tenure.
Company Time Utilization approximately 80%; 7 drivers appear 4 times in bottom-5 despite at least 5 years tenure.
Accident rate approximately 0.2% over 3 years; around 35% at-fault, around 40% preventable.
Average truck MPG approximately 6.45, with no observable improvement across three years.

Actionable Recommendations
Driver–truck assignment optimization: Match drivers with routes where they historically perform well; reserve reliable drivers for time-critical lanes.
Targeted training & coaching: Use bottom-5 lists to prioritize remedial coaching; review at-fault and preventable incident types to design targeted safety modules.
Workforce management: Combine utilization and On-time performance metrics to inform hiring, reallocation or offboarding decisions.
Fleet maintenance & replacement: Monitor per-truck MPG and maintenance records to prioritize refresh or rebuild decisions.

SQL Example
Monthly On-time performance by company scale
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
