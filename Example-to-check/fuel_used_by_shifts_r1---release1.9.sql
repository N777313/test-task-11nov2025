select count(*) from mv_history_fuel_temp4 mhft ;

SELECT * FROM public.fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-21',
    pendshift := 1
);
/*
result:
SQL Error [42702]: ERROR: column reference "eqmt" is ambiguous¶  Detail: It could refer to either a PL/pgSQL variable or a 
table column.¶  Where: PL/pgSQL function fuel_used_by_shifts_r1(integer,date,integer,date,integer,numeric,numeric) 
line 32 at RETURN QUERY
*/
--release 1.9

CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r1(
    peqmt integer,
    pstartdate date,
    pstartshift integer,
    penddate date,
    pendshift integer,
    refuel_threshold numeric DEFAULT 150,   -- рост > этого порога = заправка (л)
    max_step_drop numeric DEFAULT 200      -- абсолютная граница для шага (л) — всё что больше игнорируем
)
RETURNS TABLE(
    eqmt integer,
    fuel_used numeric,
    fuel_refueled numeric,
    valid_points integer,
    total_points integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
BEGIN
    -- Получаем границы смен (в timestamp без tz)
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
      ВАЖНО: v_history/v_view могут хранить timecreated как timestamp with time zone (timestamptz).
      Мы приведём left/right к timestamptz для корректного сравнения.
    */

    RETURN QUERY
    WITH base AS (
        SELECT 
            timecreated::timestamptz AS tstamp,
            liters,
            liters_prev,
            (liters_prev - liters) AS diff
        FROM public.mv_history_fuel_temp4
        WHERE eqmt = peqmt::text
          AND timecreated::timestamptz BETWEEN v_start_time::timestamptz AND v_end_time::timestamptz
          AND liters IS NOT NULL
          AND liters_prev IS NOT NULL
        ORDER BY timecreated
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
            SUM(clean_drop) AS fuel_used,
            SUM(clean_rise) AS fuel_refueled
        FROM filtered
    )
    SELECT
        peqmt AS eqmt,
        ROUND(COALESCE(fuel_used,0)::numeric, 1) AS fuel_used,
        ROUND(COALESCE(fuel_refueled,0)::numeric, 1) AS fuel_refueled,
        COALESCE(valid_points,0) AS valid_points,
        COALESCE(total_points,0) AS total_points
    FROM agg;
END;
$$;
