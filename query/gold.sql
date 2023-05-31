-- Driver Earning over Month and Year
CREATE OR REPLACE VIEW gold_driver_earning_month_year AS
SELECT
    drivers.driver_id AS driver_id,
    drivers.name AS driver_name,
    dim_date.month AS month,
    dim_date.year AS year,
    SUM(driver_earnings.total_amount) AS total_earnings
FROM
    driver_earnings
    JOIN
    dim_date ON driver_earnings.earning_date_key = dim_date.date_key
    JOIN
    drivers ON driver_earnings.driver_id = drivers.driver_id
GROUP BY
    drivers.driver_id, drivers.name, dim_date.month, dim_date.year
ORDER BY
    drivers.driver_id, dim_date.month

-- Driver Earning over Year
CREATE OR REPLACE VIEW gold_driver_earning_year AS
SELECT
    drivers.name AS driver_name,
    dim_date.year AS year,
    SUM(driver_earnings.total_amount) AS total_earnings
FROM
    driver_earnings
    JOIN
    dim_date ON driver_earnings.earning_date_key = dim_date.date_key
    JOIN
    drivers ON driver_earnings.driver_id = drivers.driver_id
GROUP BY
    drivers.name, dim_date.year
ORDER BY
    drivers.name, dim_date.year


-- top 5 drivers who made the most money on tips:
CREATE OR REPLACE VIEW gold_driver_who_made_most_money_on_tips AS
SELECT
    drivers.driver_id,
    drivers.name AS driver_name,
    SUM(driver_earnings.tip_amount) AS total_tips
FROM
    driver_earnings
        JOIN
    drivers ON driver_earnings.driver_id = drivers.driver_id
GROUP BY
    drivers.driver_id, drivers.name
ORDER BY
    total_tips DESC


-- Rides uber did in given month and year
CREATE OR REPLACE VIEW gold_rides_uber_month_year AS
SELECT
    YEAR(ride_date) AS year,
    MONTH(ride_date) AS month,
    COUNT(ride_id) AS ride_count
FROM
    rides
GROUP BY
    YEAR(ride_date),
    MONTH(ride_date)
ORDER BY
    year,
    month;

