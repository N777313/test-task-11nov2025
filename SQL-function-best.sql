with main as (
SELECT 
    date_trunc('hour', to_timestamp(time_created / 1000)) 
        + (date_part('minute', to_timestamp(time_created / 1000))::int / 20) * interval '20 min' AS tm,
    eqmt,
    liters 
FROM history_fuel
WHERE time_created / 1000 BETWEEN extract(epoch from timestamp '2020-11-01 00:00:00')
                              AND extract(epoch from timestamp '2025-11-04 23:59:59')
),
m2 as (
	select tm, eqmt , avg(liters) AS fuel from main group by tm, eqmt order by 2,1)
select count(*)  from m2;
--34782


-- 4) version

select count(*) from (
SELECT 
    date_trunc('hour', to_timestamp(time_created / 1000)) 
        + (date_part('minute', to_timestamp(time_created / 1000))::int / 20) * interval '20 min' AS tm,
    eqmt,
    avg(liters) AS fuel
FROM history_fuel
WHERE time_created / 1000 BETWEEN extract(epoch from timestamp '2020-11-01 00:00:00')
                              AND extract(epoch from timestamp '2025-11-04 23:59:59')
GROUP BY 
    date_trunc('hour', to_timestamp(time_created / 1000)) 
        + (date_part('minute', to_timestamp(time_created / 1000))::int / 20) * interval '20 min',
    eqmt
ORDER BY eqmt, tm) dd2;


-- 2) версия 
DO $$
DECLARE 
    timebegin double precision := extract(epoch from timestamp '2025-11-01 00:00:00');
    timeend   double precision := extract(epoch from timestamp '2025-11-04 23:59:59');
BEGIN
    PERFORM 
        date_trunc('hour', to_timestamp(time_created / 1000)) 
            + (date_part('minute', to_timestamp(time_created / 1000))::int / 20) * interval '20 min' AS tm,
        eqmt,
        avg(liters) AS fuel
    FROM history_fuel
    WHERE time_created / 1000 BETWEEN timebegin AND timeend
    GROUP BY 
        date_trunc('hour', to_timestamp(time_created / 1000)) 
            + (date_part('minute', to_timestamp(time_created / 1000))::int / 20) * interval '20 min',
        eqmt;
END $$;


-- 1 версиясы бирак бул жумыс истемейди
declare
timebegin = '';
timeend = '';
SELECT 
    date_trunc('hour', to_timestamp(time_created/1000)) 
        + date_part('minute', to_timestamp(time_created/1000))::int/20 * interval '20 min' AS tm,
    eqmt,
    avg(liters) AS fuel
FROM history_fuel
WHERE time_created/1000 BETWEEN timebegin AND timeend
GROUP BY 
    date_trunc('hour', to_timestamp(time_created/1000)) 
        + date_part('minute', to_timestamp(time_created/1000))::int/20 * interval '20 min',
    eqmt;
    
   
   
   
   
   
   
   
   
   
   
   
   
select tm, eqmt::text, fuel,
       lead(fuel,1) over (partition by eqmt order by tm desc) fuel_prev
from (
  SELECT
    date_trunc('hour', to_timestamp(time_created/1000)) 
    + date_part('minute', to_timestamp(time_created/1000))::int/20*interval '20 min' as tm,
    eqmt,
    avg(liters) as fuel
  from history_fuel
  where time_created / 1000 BETWEEN extract(epoch from timestamp '2020-11-01 00:00:00')
                              AND extract(epoch from timestamp '2025-11-04 23:59:59')
  group by tm, eqmt
) f1 order by 2,1;
   
   
   --prev
   SELECT 
    date_trunc('hour', to_timestamp(time_created / 1000)) 
        + (date_part('minute', to_timestamp(time_created / 1000))::int / 20) * interval '20 min' AS tm,
    eqmt,
    avg(liters) AS fuel
FROM history_fuel
WHERE time_created / 1000 BETWEEN extract(epoch from timestamp '2020-11-01 00:00:00')
                              AND extract(epoch from timestamp '2025-11-04 23:59:59')
GROUP BY 
    date_trunc('hour', to_timestamp(time_created / 1000)) 
        + (date_part('minute', to_timestamp(time_created / 1000))::int / 20) * interval '20 min',
    eqmt
ORDER BY eqmt, tm;






-- works
with main as (
select tm, eqmt::text, fuel,
       lead(fuel,1) over (partition by eqmt order by tm desc) fuel_prev
from (
  SELECT
    date_trunc('hour', to_timestamp(time_created/1000)) 
    + date_part('minute', to_timestamp(time_created/1000))::int/20*interval '20 min' as tm,
    eqmt,
    avg(liters) as fuel
  from history_fuel
  where time_created / 1000 BETWEEN extract(epoch from timestamp '2020-11-01 00:00:00')
                              AND extract(epoch from timestamp '2025-11-04 23:59:59')
  group by tm, eqmt
) f1 order by 2,1
),
m1 as (
select eqmt,
  case 
    when perc_same > perc_no_data or perc_jump > perc_need_cab 
      then null 
    else fuel_use 
  end as fuel_used,
  case 
    when perc_same > perc_no_data then 'no_data'
    when perc_jump > perc_need_cab then 'need calibration'
    else 'Ok'
  end as status,
  round(100 * case when perc_same > perc_no_data then null else perc_jump end, 1) as accuracy
from (
  select eqmt,
    sum(case 
          when fuel_prev - fuel > 0 and fuel_prev - fuel < max_fuel_diff 
            then fuel_prev - fuel 
          else 0 
        end) as fuel_use,
    sum(case when fuel_prev = fuel then 1.0 else 0.0 end) / count(*) as perc_same,
    sum(case 
          when fuel_prev - fuel > max_fuel_diff 
            or (fuel_prev - fuel < -max_fuel_diff and fuel_prev - fuel > -min_fueling_level) 
            then 1.0 
          else 0.0 
        end) / count(*) as perc_jump
  from main
  where fuel > 0 and fuel_prev > 0
  group by eqmt
) f2
)
select  * from m1;



--


WITH params AS (
  SELECT
    50.0 AS max_fuel_diff,       -- максимальное допустимое уменьшение топлива
    10.0 AS min_fueling_level,   -- минимальный уровень заправки, чтобы считать за “правильную”
    0.5  AS perc_no_data,        -- порог, когда считать данных нет
    0.3  AS perc_need_cab        -- порог, когда нужна калибровка
),
main AS (
  SELECT tm, eqmt::text, fuel,
         lead(fuel,1) OVER (PARTITION BY eqmt ORDER BY tm DESC) AS fuel_prev
  FROM (
    SELECT
      date_trunc('hour', to_timestamp(time_created/1000)) 
      + date_part('minute', to_timestamp(time_created/1000))::int/20*interval '20 min' AS tm,
      eqmt,
      avg(liters) AS fuel
    FROM history_fuel
    WHERE time_created / 1000 BETWEEN extract(epoch FROM timestamp '2020-11-01 00:00:00')
                                AND extract(epoch FROM timestamp '2025-11-04 23:59:59')
    GROUP BY tm, eqmt
  ) f1
  ORDER BY eqmt, tm
),
m1 AS (
SELECT eqmt,
       CASE 
         WHEN perc_same > p.perc_no_data OR perc_jump > p.perc_need_cab
           THEN NULL
         ELSE fuel_use
       END AS fuel_used,
       CASE
         WHEN perc_same > p.perc_no_data THEN 'no_data'
         WHEN perc_jump > p.perc_need_cab THEN 'need calibration'
         ELSE 'Ok'
       END AS status,
       ROUND(100 * CASE WHEN perc_same > p.perc_no_data THEN NULL ELSE perc_jump END, 1) AS accuracy
FROM (
  SELECT eqmt,
         SUM(CASE 
               WHEN fuel_prev - fuel > 0 AND fuel_prev - fuel < p.max_fuel_diff 
                 THEN fuel_prev - fuel 
               ELSE 0
             END) AS fuel_use,
         SUM(CASE WHEN fuel_prev = fuel THEN 1.0 ELSE 0.0 END) / COUNT(*) AS perc_same,
         SUM(CASE 
               WHEN fuel_prev - fuel > p.max_fuel_diff
                 OR (fuel_prev - fuel < -p.max_fuel_diff AND fuel_prev - fuel > -p.min_fueling_level)
                 THEN 1.0
               ELSE 0.0
             END) / COUNT(*) AS perc_jump
  FROM main, params p
  WHERE fuel > 0 AND fuel_prev > 0
  GROUP BY eqmt
) f2, params p
)
SELECT * FROM m1;




-- with params 20 min
WITH params AS (
  SELECT
    50.0 AS max_fuel_diff,
    10.0 AS min_fueling_level,
    0.5  AS perc_no_data,
    0.3  AS perc_need_cab,
    '2021-02-10 00:00:00'::timestamp AS tbegin,
    '2021-02-11 23:59:59'::timestamp AS tend,
    25 AS pareqmt
),
main AS (
  SELECT tm, eqmt::text, fuel,
         lead(fuel,1) OVER (PARTITION BY eqmt ORDER BY tm DESC) AS fuel_prev
  FROM (
    SELECT
      date_trunc('hour', to_timestamp(time_created/1000)) 
      + (date_part('minute', to_timestamp(time_created/1000))::int / 20) * interval '20 min' AS tm,
      eqmt,
      avg(liters) AS fuel
    FROM history_fuel, params p
    WHERE time_created / 1000 BETWEEN extract(epoch FROM p.tbegin)
                                AND extract(epoch FROM p.tend)
    GROUP BY tm, eqmt
  ) f1
),
m1 AS (
  SELECT eqmt,
         CASE 
           WHEN perc_same > p.perc_no_data OR perc_jump > p.perc_need_cab
             THEN NULL
           ELSE fuel_use
         END AS fuel_used,
         CASE
           WHEN perc_same > p.perc_no_data THEN 'no_data'
           WHEN perc_jump > p.perc_need_cab THEN 'need calibration'
           ELSE 'Ok'
         END AS status,
         ROUND(100 * CASE WHEN perc_same > p.perc_no_data THEN NULL ELSE perc_jump END, 1) AS accuracy,
         p.pareqmt
  FROM (
    SELECT eqmt,
           SUM(CASE 
                 WHEN fuel_prev - fuel > 0 AND fuel_prev - fuel < p.max_fuel_diff 
                   THEN fuel_prev - fuel 
                 ELSE 0 
               END) AS fuel_use,
           SUM(CASE WHEN fuel_prev = fuel THEN 1.0 ELSE 0.0 END) / COUNT(*) AS perc_same,
           SUM(CASE 
                 WHEN fuel_prev - fuel > p.max_fuel_diff
                   OR (fuel_prev - fuel < -p.max_fuel_diff AND fuel_prev - fuel > -p.min_fueling_level)
                   THEN 1.0
                 ELSE 0.0
               END) / COUNT(*) AS perc_jump
    FROM main, params p
    WHERE fuel > 0 AND fuel_prev > 0
    GROUP BY eqmt
  ) f2, params p
)
SELECT * 
FROM m1
WHERE eqmt::int = pareqmt;



-----------------------------------
--		SQL Function the Best - works good
-- with params 20 min      
WITH params AS (
  SELECT
    50.0 AS max_fuel_diff,
    10.0 AS min_fueling_level,
    0.5  AS perc_no_data,
    0.3  AS perc_need_cab,
    '2021-02-10 00:00:00'::timestamp AS tbegin,
    '2021-02-11 23:59:59'::timestamp AS tend,
    25 AS pareqmt
),
main AS (
  SELECT tm, eqmt::text, fuel,
         lead(fuel,1) OVER (PARTITION BY eqmt ORDER BY tm DESC) AS fuel_prev
  FROM (
    SELECT
      date_trunc('hour', to_timestamp(time_created/1000)) 
      + (date_part('minute', to_timestamp(time_created/1000))::int / 10) * interval '10 min' AS tm,
      eqmt,
      avg(liters) AS fuel
    FROM history_fuel, params p
    WHERE time_created / 1000 BETWEEN extract(epoch FROM p.tbegin)
                                AND extract(epoch FROM p.tend)
    GROUP BY tm, eqmt
  ) f1
),
m1 AS (
  SELECT eqmt,
         CASE 
           WHEN perc_same > p.perc_no_data OR perc_jump > p.perc_need_cab
             THEN NULL
           ELSE fuel_use
         END AS fuel_used,
         CASE
           WHEN perc_same > p.perc_no_data THEN 'no_data'
           WHEN perc_jump > p.perc_need_cab THEN 'need calibration'
           ELSE 'Ok'
         END AS status,
         ROUND(100 * CASE WHEN perc_same > p.perc_no_data THEN NULL ELSE perc_jump END, 1) AS accuracy,
         p.pareqmt
  FROM (
    SELECT eqmt,
           SUM(CASE 
                 WHEN fuel_prev - fuel > 0 AND fuel_prev - fuel < p.max_fuel_diff 
                   THEN fuel_prev - fuel 
                 ELSE 0 
               END) AS fuel_use,
           SUM(CASE WHEN fuel_prev = fuel THEN 1.0 ELSE 0.0 END) / COUNT(*) AS perc_same,
           SUM(CASE 
                 WHEN fuel_prev - fuel > p.max_fuel_diff
                   OR (fuel_prev - fuel < -p.max_fuel_diff AND fuel_prev - fuel > -p.min_fueling_level)
                   THEN 1.0
                 ELSE 0.0
               END) / COUNT(*) AS perc_jump
    FROM main, params p
    WHERE fuel > 0 AND fuel_prev > 0
    GROUP BY eqmt
  ) f2, params p
)
SELECT * 
FROM m1
WHERE eqmt::int = pareqmt;





--##########################		Analyze SQL
-----------------------------------
--		SQL Function the Best - works good
-- with params 20 min      
WITH params AS (
  SELECT
    50.0 AS max_fuel_diff,
    10.0 AS min_fueling_level,
    0.5  AS perc_no_data,
    0.3  AS perc_need_cab,
    '2021-02-10 00:00:00'::timestamp AS tbegin,
    '2021-02-11 23:59:59'::timestamp AS tend,
    25 AS pareqmt
),
main AS (
  SELECT tm, eqmt::text, fuel,
         lead(fuel,1) OVER (PARTITION BY eqmt ORDER BY tm DESC) AS fuel_prev
  FROM (
    SELECT
      date_trunc('hour', to_timestamp(time_created/1000)) + (date_part('minute', to_timestamp(time_created/1000))::int / 5) * interval '5 min' AS tm,
      eqmt,
      avg(liters) AS fuel
    FROM history_fuel, params p
    WHERE time_created / 1000 BETWEEN extract(epoch FROM p.tbegin)
                                AND extract(epoch FROM p.tend)
    GROUP BY tm, eqmt
  ) f1
)
SELECT tm, eqmt,  ROUND(fuel::numeric, 3) as FUEL, ROUND(fuel_prev::numeric, 3) as fuel_prev
FROM main, params p 
WHERE eqmt::int = p.pareqmt order by 2,1;




select max(liters) from history_fuel hf ;

select distinct liters, eqmt  from history_fuel hf order by 1 desc;

select max(liters), eqmt from (
select distinct liters, eqmt  from history_fuel hf order by 1 desc) d2 group by eqmt order by 2;

select distinct raw  from history_fuel hf ;  --0

select * from shifts s order by 1,2;