select 'a' as b;

SELECT *
FROM public.fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-21',
    pendshift := 1
);


--2.1
CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r1(
    peqmt integer,
    pstartdate date,
    pstartshift integer,
    penddate date,
    pendshift integer,
    refuel_threshold numeric DEFAULT 150,   -- рост > этого порога = заправка (л)
    max_step_drop numeric DEFAULT 200      -- абсолютная граница для шага (л)
)
RETURNS TABLE(
    eqmt integer,
    total_used numeric,
    total_refueled numeric,
    valid_points integer,
    total_points integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
BEGIN
    -- 1. Получаем время начала смены
    SELECT shiftstart
    INTO v_start_time
    FROM shifts
    WHERE shiftdate = pstartdate AND shift = pstartshift
    LIMIT 1;

    -- 2. Время конца смены
    SELECT shiftstart + interval '12 hour'
    INTO v_end_time
    FROM shifts
    WHERE shiftdate = penddate AND shift = pendshift
    LIMIT 1;

    -- Проверка
    IF v_start_time IS NULL OR v_end_time IS NULL THEN
        RAISE EXCEPTION 'Не найдены границы смен (start: %, end: %)', v_start_time, v_end_time;
    END IF;

    -- Если конец смены раньше начала — добавляем день
    IF v_end_time <= v_start_time THEN
        v_end_time := v_end_time + interval '1 day';
    END IF;
   -- отти
	IF 1 = 1 then
		RAISE WARNING 'Passed 1.';
    END IF;

    -- 3. Основная логика
    RETURN QUERY
    WITH base AS (
        SELECT 
            v.timecreated::timestamptz AS tstamp,
            v.liters,
            v.liters_prev,
            (v.liters_prev - v.liters) AS diff
        FROM public.v_history_fuel_temp4 v
        WHERE v.eqmt = peqmt::text
          AND v.timecreated::timestamptz BETWEEN v_start_time::timestamptz AND v_end_time::timestamptz
          AND v.liters IS NOT NULL
          AND v.liters_prev IS NOT NULL
        ORDER BY v.timecreated
    ),
    filtered AS (
        SELECT
            *,
            CASE WHEN diff > 0 AND diff <= max_step_drop THEN diff ELSE 0 END AS clean_drop,
            CASE WHEN diff < 0 AND ABS(diff) > refuel_threshold AND ABS(diff) <= max_step_drop THEN ABS(diff) ELSE 0 END AS clean_rise
        FROM base
    ),
    agg AS (
        SELECT 
            COUNT(*) AS total_points,
            COUNT(*) FILTER (WHERE clean_drop > 0 OR clean_rise > 0) AS valid_points,
            SUM(clean_drop) AS used_sum,
            SUM(clean_rise) AS refueled_sum
        FROM filtered
    )
    SELECT
        peqmt AS eqmt,
        ROUND(COALESCE(used_sum,0)::numeric, 1) AS total_used,
        ROUND(COALESCE(refueled_sum,0)::numeric, 1) AS total_refueled,
        COALESCE(valid_points,0) AS valid_points,
        COALESCE(total_points,0) AS total_points
    FROM agg;
   
	IF 1 = 1 then
		RAISE WARNING 'End of story bye bye.)))';
    END IF;
END;
$$;
