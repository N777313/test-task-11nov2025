create or replace view v_history_fuel_temp2 as 
WITH params AS (
  SELECT
    50.0 AS max_fuel_diff,
    10.0 AS min_fueling_level,
    0.5  AS perc_no_data,
    0.3  AS perc_need_cab,
    '2018-02-10 00:00:00'::timestamp AS tbegin,
    '2025-11-05 23:59:59'::timestamp AS tend,
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
--WHERE eqmt::int = p.pareqmt 
order by 2,1;


--###################################################################################
--next version
create or replace view v_history_fuel_temp3 as 
WITH params AS (
  SELECT
    50.0 AS max_fuel_diff,
    10.0 AS min_fueling_level,
    0.5  AS perc_no_data,
    0.3  AS perc_need_cab,
    '2018-02-10 00:00:00'::timestamp AS tbegin,
    '2025-11-05 23:59:59'::timestamp AS tend,
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
--    WHERE time_created / 1000 BETWEEN extract(epoch FROM p.tbegin) AND extract(epoch FROM p.tend)
    GROUP BY tm, eqmt
  ) f1
)
SELECT tm, eqmt,  ROUND(fuel::numeric, 3) as FUEL, ROUND(fuel_prev::numeric, 3) as fuel_prev
FROM main, params p 
--WHERE eqmt::int = p.pareqmt 
order by 2,1;


--####################################################################################
select count(*)  from v_history_fuel_temp;	--182
select count(*)  from v_history_fuel_temp2;	--137420


select * from public.v_history_fuel_temp2 limit 2;
select distinct eqmt from v_history_fuel_temp2;




CREATE INDEX idx_history_fuel_eqmt_time ON history_fuel(eqmt, to_timestamp(time_created/1000));




--##################################################
select * from shifts s where shiftdate <= '2021-03-30' order by 1 desc ;
--start:    2021-03-30 08:45:00.000
--end: 		2021-03-29 20:45:00.000
select * from v_history_fuel_temp2 vhft order by 1 desc;


select max(tm) as dt from v_history_fuel_temp2 vhft;
select min(tm) as dt from v_history_fuel_temp2 vhft;

select min(date_trunc(tm)) as dt, eqmt  from v_history_fuel_temp2 vhft group by date_trunc(tm) , eqmt ;
SELECT 
    max(tm::date) AS dt, 
    eqmt
FROM v_history_fuel_temp2 vhft
group by  eqmt ;


SELECT 
    MIN(tm::date) AS dt,
    eqmt
FROM v_history_fuel_temp2 vhft
GROUP BY eqmt
ORDER BY eqmt;


SELECT 
    MIN(time_created_dt::date) AS dt,
    eqmt
FROM (select TO_TIMESTAMP(1616188691898 / 1000.0) AS time_created_dt,* from history_fuel) vhft
GROUP BY eqmt
ORDER BY eqmt;

SELECT 
    MAX(time_created_dt::date) AS dt,
    eqmt
FROM (select TO_TIMESTAMP(1616188691898 / 1000.0) AS time_created_dt,* from history_fuel) vhft
GROUP BY eqmt
ORDER BY eqmt;

select count(*) from history_fuel hf ; --1448308


--###################################################
create or replace view v_history_fuel_r2 as  
select 
	date_trunc('hour', to_timestamp(time_created/1000)) + (date_part('minute', to_timestamp(time_created/1000))::int / 5) * interval '5 min' as time_created_5m,
	TO_TIMESTAMP(1616188691898 / 1000.0) AS time_created_convert,
	* 
from history_fuel order by 4,2;

select * from shifts where shiftstart::date >= '2021-03-20' order by 1;

--2021-03-20 08:45:00.000
--2021-03-20 20:45:00.000

select * from v_history_fuel_r1 where eqmt = 25 and liters > 0;
select time_created_5m,time_created_convert,time_created,eqmt,liters from v_history_fuel_r2 where eqmt = 25 and liters > 0;

select 
	time_created_5m,time_created_convert,time_created,eqmt,liters 
from v_history_fuel_r2 
where eqmt = 25 
	and time_created / 1000 BETWEEN extract(epoch FROM '2021-03-20 08:45:00.000') AND extract(epoch FROM '2021-03-20 20:45:00.000');
	

SELECT 
    time_created_5m,
    time_created_convert,
    time_created,
    eqmt,
    liters 
FROM v_history_fuel_r2 
WHERE eqmt = 25 
  AND time_created / 1000 BETWEEN 
        extract(epoch FROM '2021-03-20 08:45:00.000') 
    AND extract(epoch FROM '2021-03-20 20:45:00.000');

   
SELECT 
    time_created_5m,
    time_created_convert,
    time_created,
    eqmt,
    liters 
FROM v_history_fuel_r2 
WHERE eqmt = 25 
  AND time_created / 1000 BETWEEN 
        extract(epoch FROM TIMESTAMP '2021-03-20 08:45:00.000') 
    AND extract(epoch FROM TIMESTAMP '2021-03-20 20:45:00.000')
order by 2;

SELECT tm, eqmt::text, fuel,
         lead(fuel,1) OVER (PARTITION BY eqmt ORDER BY tm DESC) AS fuel_prev
  FROM (
    SELECT
      date_trunc('hour', to_timestamp(time_created/1000)) + (date_part('minute', to_timestamp(time_created/1000))::int / 5) * interval '5 min' AS tm,
      eqmt,
      avg(liters) AS fuel
    FROM history_fuel, params p
--    WHERE time_created / 1000 BETWEEN extract(epoch FROM p.tbegin) AND extract(epoch FROM p.tend)
    GROUP BY tm, eqmt
  ) f1
  --####################################################

  
  
  
SELECT 
	time_created_5m, 
	eqmt::text, 
	liters,
	lead(liters,1) OVER (PARTITION BY eqmt ORDER BY time_created_5m DESC) AS fuel_prev
from (
SELECT 
    time_created_5m,
    time_created_convert,
    time_created,
    eqmt,
    liters 
FROM v_history_fuel_r2 
WHERE eqmt = 25 
  AND time_created / 1000 BETWEEN 
        extract(epoch FROM TIMESTAMP '2021-03-20 08:45:00.000') 
    AND extract(epoch FROM TIMESTAMP '2021-03-20 20:45:00.000')
order by 2) D
group by D.eqmt,D.time_created_5m
order by 1;






create or replace view v_history_fuel_temp4 as 
WITH main AS (
  SELECT 
  	timecreated, 
  	eqmt::text, 
  	liters,
  	lead(liters,1) OVER (PARTITION BY eqmt ORDER BY timecreated DESC) AS liters_prev
  FROM (
    SELECT
      date_trunc('hour', to_timestamp(time_created/1000)) + (date_part('minute', to_timestamp(time_created/1000))::int / 5) * interval '5 min' AS timecreated,
      eqmt,
      avg(liters) AS liters
    FROM history_fuel
    GROUP BY timecreated, eqmt
  ) f1
)
SELECT timecreated, eqmt,  ROUND(liters::numeric, 3) as liters, ROUND(liters_prev::numeric, 3) as liters_prev
FROM main  
order by 2,1;

select * from v_history_fuel_temp4 where eqmt = '25';
select * from v_history_fuel_temp4 
where 
time_created / 1000 BETWEEN 
        extract(TIMESTAMP '2021-03-20 08:45:00.000') 
    AND extract(TIMESTAMP '2021-03-20 20:45:00.000')
and eqmt = '25'; --13151

--CREATE INDEX idx_mv_history_fuel_temp4_eqmt_time ON mv_history_fuel_temp4 (eqmt, timecreated);

--CREATE MATERIALIZED VIEW mv_history_fuel_temp4 as SELECT * FROM v_history_fuel_temp4;

-- Good example of to check the data
select * from mv_history_fuel_temp4 
WHERE 
	eqmt = '25'
  	AND timecreated BETWEEN TIMESTAMP '2021-03-20 08:45:00' AND TIMESTAMP '2021-03-20 20:45:00' order by 1;
  	
  
  
  
  
  
  
  
  --###########		Main function ############################################
  CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r1(
    peqmt integer,
    pstartdate date,
    pstartshift integer,
    penddate date,
    pendshift integer
)
RETURNS numeric AS
$$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_fuel_used numeric;
BEGIN
    -- 1️⃣ Определяем время начала первой смены
    SELECT shiftstart
    INTO v_start_time
    FROM shifts
    WHERE shiftdate = pstartdate
      AND shift = pstartshift;

    -- 2️⃣ Определяем время конца последней смены (+12 часов)
    SELECT shiftstart + interval '12 hour'
    INTO v_end_time
    FROM shifts
    WHERE shiftdate = penddate
      AND shift = pendshift;

    -- Проверка корректности дат
    IF v_start_time IS NULL OR v_end_time IS NULL THEN
        RAISE EXCEPTION 'Не найдены границы смен (start: %, end: %)', v_start_time, v_end_time;
    END IF;

    -- 3️⃣ Расчет расхода топлива
    SELECT 
        ROUND(SUM(liters_prev - liters) FILTER (WHERE liters_prev > liters), 1)
    INTO v_fuel_used
    FROM v_history_fuel_temp4
    WHERE eqmt = peqmt
      AND to_timestamp(timecreated / 1000) BETWEEN v_start_time AND v_end_time;

    -- 4️⃣ Возврат результата
    RETURN COALESCE(v_fuel_used, 0);
END;
$$ LANGUAGE plpgsql;

  --###########		end function  ############################################



--	###########			check function results			######################
--Пример вызова функции

SELECT fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-20',
    pendshift := 4
);


SELECT fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-20',
    pendshift := 1
);

SELECT fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-20',
    pendshift := 1
);



-- Good example of to check the data
select * from mv_history_fuel_temp4 
WHERE 
	eqmt = '25'
  	AND timecreated BETWEEN TIMESTAMP '2021-03-20 08:45:00' AND TIMESTAMP '2021-03-20 20:45:00' order by 1;

select * from shifts where shiftstart::date >= '2021-03-20' order by 1;
--2021-03-20 08:45:00' AND TIMESTAMP '2021-03-20 20:45:00' order by 1;
--############################################################################




--№№№№№
SELECT shiftdate, shift, shiftstart
FROM shifts
WHERE shiftdate = '2021-03-20';
