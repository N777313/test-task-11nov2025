select 'Release 2.2' as rel;


SELECT * FROM fuel_used_by_nur(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-21',
    pendshift := 1
);



--Main release 2.1
CREATE OR REPLACE FUNCTION public.fuel_used_by_nur(
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
begin
    ----------------------------------------------------------------
    -- Создание материализованного представления
    ----------------------------------------------------------------
    RAISE NOTICE 'Создаём материализованное представление mv_history_fuel_temp4...';

    EXECUTE '
	DROP MATERIALIZED VIEW IF EXISTS public.mv_history_fuel_temp4;
    CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_history_fuel_temp4 AS
    WITH main AS (
        SELECT 
            f1.timecreated,
            f1.eqmt::text AS eqmt,
            f1.liters,
            lead(f1.liters, 1) OVER (PARTITION BY f1.eqmt ORDER BY f1.timecreated DESC) AS liters_prev
        FROM (
            SELECT 
                date_trunc(''hour'', to_timestamp((history_fuel.time_created / 1000)::double precision))
                + (date_part(''minute'', to_timestamp((history_fuel.time_created / 1000)::double precision))::integer / 5)::double precision 
                * interval ''5 minutes'' AS timecreated,
                history_fuel.eqmt,
                avg(history_fuel.liters) AS liters
            FROM history_fuel
            GROUP BY 1, history_fuel.eqmt
        ) f1
    )
    SELECT 
        main.timecreated,
        main.eqmt,
        round(main.liters::numeric, 3) AS liters,
        round(main.liters_prev::numeric, 3) AS liters_prev
    FROM main
    ORDER BY main.eqmt, main.timecreated
    ';

    EXECUTE 'DROP INDEX IF EXISTS idx_mv_history_fuel_temp4_eqmt_time;
	CREATE INDEX IF NOT EXISTS idx_mv_history_fuel_temp4_eqmt_time 
              ON public.mv_history_fuel_temp4 (eqmt, timecreated);';

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
    -- Main part
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


    RAISE NOTICE 'Удаляем временное представление mv_history_fuel_temp4...';
    EXECUTE 'DROP INDEX IF EXISTS idx_mv_history_fuel_temp4_eqmt_time;';
    EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS public.mv_history_fuel_temp4;';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Ошибка: %, выполняется очистка временных объектов...', SQLERRM;
        EXECUTE 'DROP INDEX IF EXISTS idx_mv_history_fuel_temp4_eqmt_time;';
        EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS public.mv_history_fuel_temp4;';
        RAISE;
END;
$function$;