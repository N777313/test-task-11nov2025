select 'Release 2.1' as rel;

SELECT * FROM fuel_used_by_shifts_r2_1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-21',
    pendshift := 1
);



--Main release 2.1
CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r2_1(
    peqmt integer,
    pstartdate date,
    pstartshift integer,
    penddate date,
    pendshift integer,
    perc_no_data numeric DEFAULT 80,
    perc_need_cab numeric DEFAULT 30
)
RETURNS TABLE(
    eqmt_id integer,
    time_from timestamp without time zone,
    time_to timestamp without time zone,
    fuel_used numeric,
    --valid_points bigint,
    status text
)
LANGUAGE plpgsql
AS $function$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
BEGIN
    ----------------------------------------------------------------
    -- Определяем начало и конец интервала по таблице shifts
    ----------------------------------------------------------------
    SELECT s.shiftstart
      INTO v_start_time
      FROM shifts s
     WHERE s.shiftdate = pstartdate
       AND s.shift = pstartshift
     LIMIT 1;

    SELECT LEAD(s.shiftstart) OVER (ORDER BY s.shiftstart)
      INTO v_end_time
      FROM shifts s
     WHERE s.shiftstart >= v_start_time
     ORDER BY s.shiftstart
     LIMIT 1;

    IF v_start_time IS NULL OR v_end_time IS NULL THEN
        RAISE EXCEPTION 'Не удалось определить границы смены (% %, % %)', pstartdate, pstartshift, penddate, pendshift;
    END IF;

   -- Финальное сообщение отладки
    RAISE NOTICE 'Shift dates v_start_time=%, v_end_time=%.', v_start_time, v_end_time;
    ----------------------------------------------------------------
    -- Основной расчёт расхода топлива по mv_history_fuel_temp4
    ----------------------------------------------------------------
    RETURN QUERY
    WITH raw_data AS (
        SELECT 
            eqmt,
            timecreated,
            liters,
            liters_prev,
            (liters - liters_prev) AS diff
        FROM public.mv_history_fuel_temp4
        WHERE eqmt = peqmt::text
          AND timecreated >= v_start_time
          AND timecreated < v_end_time
    ),
    calc AS (
        SELECT
            eqmt,
            SUM(CASE WHEN diff < 0 THEN abs(diff) ELSE 0 END) AS fuel_used,
            COUNT(*) AS valid_points,
            CASE 
                WHEN COUNT(*) = 0 THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > perc_no_data THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > perc_need_cab THEN 'need calibration'
                ELSE 'ok'
            END AS status
        FROM raw_data
        GROUP BY eqmt
    )
    SELECT 
        peqmt AS eqmt_id,
        v_start_time AS time_from,
        v_end_time AS time_to,
        calc.fuel_used,
        --calc.valid_points,
        calc.status
    FROM calc;
END;
$function$;



--check data manually			SQL works
    with params as (
    select 
    	25 as peqmt,
    	'2021-03-20 08:45:00'::timestamptz AS v_start_time,
        '2021-03-20 20:45:00'::timestamptz AS v_end_time,
        80 as perc_no_data,
        30 as perc_need_cab 
--    	'2021-03-20' as v_start_time,
--    	'2021-03-21' as v_end_time
    ), 
    raw_data AS (
        SELECT 
            eqmt,
            timecreated,
            liters,
            liters_prev,
            (liters - liters_prev) AS diff
        FROM public.mv_history_fuel_temp4, params p1
        WHERE eqmt = p1.peqmt::text
          AND timecreated >= p1.v_start_time
          AND timecreated < p1.v_end_time
    ),
    calc AS (
        SELECT
            eqmt,
            SUM(CASE WHEN diff < 0 THEN abs(diff) ELSE 0 END) AS fuel_used,
            COUNT(*) AS valid_points,
            CASE 
                WHEN COUNT(*) = 0 THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > MAX(p2.perc_no_data) /*p2.perc_no_data */ THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > MAX(p2.perc_need_cab) /*p2.perc_need_cab*/ THEN 'need calibration'
                ELSE 'ok'
            END AS status
        FROM raw_data, params p2
        GROUP BY eqmt
    )
    SELECT 
        p3.peqmt AS eqmt_id,
        p3.v_start_time AS time_from,
        p3.v_end_time AS time_to,
        calc.fuel_used,
        calc.valid_points,
        calc.status
    FROM calc, params p3;
    
   
   
   
   
   
   
   
   
   
   
--###########################################################################
   --		view raw data only
with params as (
    select 
    	25 as peqmt,
    	'2021-03-20 08:45:00'::timestamptz AS v_start_time,
        '2021-03-20 20:45:00'::timestamptz AS v_end_time,
        80 as perc_no_data,
        30 as perc_need_cab 
--    	'2021-03-20' as v_start_time,
--    	'2021-03-21' as v_end_time
    ), 
    raw_data AS (
        SELECT 
            eqmt,
            timecreated,
            liters,
            liters_prev,
            (liters - liters_prev) AS diff
        FROM public.mv_history_fuel_temp4, params p1
        WHERE eqmt = p1.peqmt::text
          AND timecreated >= p1.v_start_time
          AND timecreated < p1.v_end_time
    ),
    calc AS (
        SELECT
            eqmt,
            SUM(CASE WHEN diff < 0 THEN abs(diff) ELSE 0 END) AS fuel_used,
            COUNT(*) AS valid_points,
            CASE 
                WHEN COUNT(*) = 0 THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > MAX(p2.perc_no_data) /*p2.perc_no_data */ THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > MAX(p2.perc_need_cab) /*p2.perc_need_cab*/ THEN 'need calibration'
                ELSE 'ok'
            END AS status
        FROM raw_data, params p2
        GROUP BY eqmt
    )
    SELECT 
        *
    FROM raw_data order by 2 ;
    
   
   
   
--###########################################################################
   --		view raw data only
with params as (
    select 
    	25 as peqmt,
    	'2021-03-20 08:45:00'::timestamptz AS v_start_time,
        '2021-03-20 20:45:00'::timestamptz AS v_end_time,
        80 as perc_no_data,
        30 as perc_need_cab 
--    	'2021-03-20' as v_start_time,
--    	'2021-03-21' as v_end_time
    ), 
    raw_data AS (
        SELECT 
            eqmt,
            timecreated,
            liters,
            liters_prev,
            (liters - liters_prev) AS diff
        FROM public.mv_history_fuel_temp4, params p1
        WHERE eqmt = p1.peqmt::text
          AND timecreated >= p1.v_start_time
          AND timecreated < p1.v_end_time
    ),
    calc AS (
        SELECT
            eqmt,
            SUM(CASE WHEN diff < 0 THEN abs(diff) ELSE 0 END) AS fuel_used,
            COUNT(*) AS valid_points,
            CASE 
                WHEN COUNT(*) = 0 THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > MAX(p2.perc_no_data) /*p2.perc_no_data */ THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > MAX(p2.perc_need_cab) /*p2.perc_need_cab*/ THEN 'need calibration'
                ELSE 'ok'
            END AS status
        FROM raw_data, params p2
        GROUP BY eqmt
    )
    SELECT
            eqmt,
            SUM(CASE WHEN diff < 0 THEN abs(diff) ELSE 0 END) AS fuel_used,
            COUNT(*) AS valid_points,
            CASE 
                WHEN COUNT(*) = 0 THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > MAX(p2.perc_no_data) /*p2.perc_no_data */ THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > MAX(p2.perc_need_cab) /*p2.perc_need_cab*/ THEN 'need calibration'
                ELSE 'ok'
            END AS status
        FROM raw_data, params p2
        GROUP BY eqmt;
        
       
       
--###########################################################################
   --		view raw data only
with params as (
    select 
    	25 as peqmt,
    	'2021-03-20 08:45:00'::timestamptz AS v_start_time,
        '2021-03-20 20:45:00'::timestamptz AS v_end_time,
        80 as perc_no_data,
        30 as perc_need_cab 
--    	'2021-03-20' as v_start_time,
--    	'2021-03-21' as v_end_time
    ), 
    raw_data AS (
        SELECT 
            eqmt,
            timecreated,
            liters,
            liters_prev,
            (liters - liters_prev) AS diff
        FROM public.mv_history_fuel_temp4, params p1
        WHERE eqmt = p1.peqmt::text
          AND timecreated >= p1.v_start_time
          AND timecreated < p1.v_end_time
    ),
    calc AS (
        SELECT
            eqmt,
            SUM(CASE WHEN diff < 0 THEN abs(diff) ELSE 0 END) AS fuel_used,
            COUNT(*) AS valid_points,
            CASE 
                WHEN COUNT(*) = 0 THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > MAX(p2.perc_no_data) /*p2.perc_no_data */ THEN 'no_data'
                WHEN SUM(CASE WHEN diff > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > MAX(p2.perc_need_cab) /*p2.perc_need_cab*/ THEN 'need calibration'
                ELSE 'ok'
            END AS status
        FROM raw_data, params p2
        GROUP BY eqmt
    )
select 
	*, 
	CASE WHEN diff < 0 THEN abs(diff) ELSE 0 END AS fuel_used 
FROM raw_data, params p2 
order by 2;









--- object drop 

SELECT proname, oid::regprocedure, *
FROM pg_proc
WHERE proname ILIKE 'fuel_used_by_shifts_r2_1';

drop function fuel_used_by_shifts_r2_1(integer,date,integer,date,integer,numeric,numeric)