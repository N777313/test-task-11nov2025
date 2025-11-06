  SELECT 
        v.eqmt,
        v.timecreated,
        v.liters,
        v.liters_prev,
        round(v.liters - v.liters_prev, 1) AS delta
    FROM mv_history_fuel_temp4 v
    WHERE v.eqmt = 25::text
      AND v.timecreated BETWEEN '2021-03-20' AND '2021-03-21'
    ORDER BY v.timecreated;
   
SELECT *
FROM fuel_used_by_shifts_r181(
    peqmt := '25',
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-21',
    pendshift := 1
);


CREATE OR REPLACE FUNCTION fuel_used_by_shifts_r181(
    peqmt integer,
    pstartdate date,
    pstartshift integer,
    penddate date,
    pendshift integer
)
RETURNS TABLE(
    eqmt text,
    timecreated timestamp,
    liters numeric,
    liters_prev numeric,
    delta numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        v.eqmt,
        v.timecreated,
        v.liters,
        v.liters_prev,
        round(v.liters - v.liters_prev, 1) AS delta
    FROM mv_history_fuel_temp4 v
    WHERE v.eqmt = peqmt::text
      AND v.timecreated BETWEEN pstartdate AND penddate
    ORDER BY v.timecreated;

    RAISE NOTICE 'Done for eqmt=%', peqmt;
END;
$$;




-- check 1
SELECT *
FROM mv_history_fuel_temp4
WHERE eqmt = '25'
  AND timecreated BETWEEN '2021-03-20 00:00' AND '2021-03-21 23:59'
ORDER BY timecreated
LIMIT 10;


--check
SELECT *
FROM fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-21',
    pendshift := 1
);


--1.8
CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r1(
    peqmt integer,
    pstartdate date,
    pstartshift integer,
    penddate date,
    pendshift integer,
    refuel_threshold numeric DEFAULT 150,   -- рост > этого порога = заправка
    max_step_drop numeric DEFAULT 200       -- падение > этого порога = аномалия
)
RETURNS TABLE(
    eqmt integer,
    fuel_used numeric,
    fuel_refueled numeric,
    valid_points integer,
    total_points integer
)
LANGUAGE plpgsql
AS $function$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
BEGIN
    -- 1️⃣ Определяем границы смен
    SELECT shiftstart
    INTO v_start_time
    FROM shifts
    WHERE shiftdate = pstartdate AND shift = pstartshift
    LIMIT 1;

    SELECT shiftstart + interval '12 hour'
    INTO v_end_time
    FROM shifts
    WHERE shiftdate = penddate AND shift = pendshift
    LIMIT 1;

    IF v_start_time IS NULL OR v_end_time IS NULL THEN
        RAISE EXCEPTION 'Не найдены границы смен (start: %, end: %)', v_start_time, v_end_time;
    END IF;

    IF v_end_time <= v_start_time THEN
        v_end_time := v_end_time + interval '1 day';
    END IF;

    /*
      2️⃣ Основная логика:
      - Берём данные из v_history_fuel_temp4 (где timecreated — timestamp)
      - Считаем Δ = liters_prev - liters
      - Исключаем скачки (>|max_step_drop|)
      - Разделяем расход и заправку
    */

    RETURN QUERY
    WITH base AS (
        SELECT 
            timecreated,
            liters,
            liters_prev,
            (liters_prev - liters) AS diff
        FROM public.v_history_fuel_temp4
        WHERE eqmt = peqmt::text
          AND timecreated BETWEEN v_start_time AND v_end_time
          AND liters IS NOT NULL
          AND liters_prev IS NOT NULL
        ORDER BY timecreated
    ),
    filtered AS (
        SELECT 
            *,
            CASE 
                WHEN diff > 0 AND diff <= max_step_drop THEN diff
                ELSE 0 
            END AS clean_drop,
            CASE 
                WHEN diff < 0 AND ABS(diff) > refuel_threshold AND ABS(diff) <= max_step_drop THEN ABS(diff)
                ELSE 0 
            END AS clean_rise
        FROM base
    ),
    agg AS (
        SELECT 
            COUNT(*) AS total_points,
            COUNT(*) FILTER (WHERE clean_drop > 0 OR clean_rise > 0) AS valid_points,
            SUM(clean_drop) AS fuel_used,
            SUM(clean_rise) AS fuel_refueled
        FROM filtered
    )
    SELECT 
        peqmt AS eqmt,
        ROUND(COALESCE(fuel_used, 0), 1),
        ROUND(COALESCE(fuel_refueled, 0), 1),
        valid_points,
        total_points
    FROM agg;
END;
$function$;






--#################################################################################
-- 182
CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r1_debug(
    peqmt integer,
    pstartdate date,
    pstartshift integer,
    penddate date,
    pendshift integer
)
RETURNS TABLE(
    eqmt text,
    timecreated timestamptz,
    liters numeric,
    liters_prev numeric,
    delta numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        v.eqmt,
        v.timecreated::timestamptz,
        v.liters,
        v.liters_prev,
        round((v.liters_prev - v.liters)::numeric, 3) AS delta
    FROM public.v_history_fuel_temp4 v
    WHERE v.eqmt = peqmt::text
      -- Используем полные границы по дате (включая сутки)
      AND v.timecreated::timestamptz
            BETWEEN (pstartdate::timestamp)::timestamptz
                AND (penddate::timestamp + interval '1 day')::timestamptz
    ORDER BY v.timecreated
    LIMIT 1000; -- ограничение, чтобы не возвращать слишком много строк

    RAISE NOTICE 'debug: returned rows for eqmt=%', peqmt;
END;
$$;



SELECT * FROM public.fuel_used_by_shifts_r1_debug(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-21',
    pendshift := 1
);
