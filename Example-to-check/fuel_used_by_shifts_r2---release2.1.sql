select 'Release 2.1' as rel;


--Main release 2.1
CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r2(
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
    valid_points integer,
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
        calc.valid_points,
        calc.status
    FROM calc;
END;
$function$;
