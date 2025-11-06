EXPLAIN ANALYZE
SELECT timecreated, liters, liters_prev
FROM v_history_fuel_temp4
WHERE eqmt = '25'
  AND timecreated BETWEEN TIMESTAMP '2021-03-20 08:45:00' AND TIMESTAMP '2021-03-20 20:45:00'
ORDER BY timecreated;


--release 1.5
SELECT fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-20',
    pendshift := 1
);
результат:
fuel_used_by_shifts_r1
1545.7
/*
 SQL Error [42725]: ERROR: function fuel_used_by_shifts_r1(peqmt => integer, pstartdate => unknown, pstartshift => integer, penddate => unknown, 
 pendshift => integer) is not unique¶  Hint: Could not choose a best candidate function. 
 You might need to add explicit type casts.¶  Position: 8 */


-- сколько строк в интервале (быстрая проверка)
SELECT count(*) 
FROM v_history_fuel_temp4
WHERE eqmt = '25' 
  AND timecreated BETWEEN TIMESTAMP '2021-03-20 08:45:00' AND TIMESTAMP '2021-03-20 20:45:00';



-- release 1.5
 CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r1(
    peqmt integer,
    pstartdate date,
    pstartshift integer,
    penddate date,
    pendshift integer,
    refuel_threshold numeric DEFAULT 150,   -- порог роста для определения заправки (литров)
    max_step_drop numeric DEFAULT 200     -- максимальный допустимый разовый спад (литров)
)
RETURNS numeric
LANGUAGE plpgsql
AS $function$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_fuel_used numeric;
BEGIN
    -- 1) начало смены
    SELECT shiftstart
    INTO v_start_time
    FROM shifts
    WHERE shiftdate = pstartdate
      AND shift = pstartshift
    LIMIT 1;

    -- 2) конец смены (берём start + 12h для указанной смены; при необходимости можно улучшить)
    SELECT shiftstart + interval '12 hour'
    INTO v_end_time
    FROM shifts
    WHERE shiftdate = penddate
      AND shift = pendshift
    LIMIT 1;

    IF v_start_time IS NULL OR v_end_time IS NULL THEN
        RAISE EXCEPTION 'Не найдены границы смен (start: %, end: %)', v_start_time, v_end_time;
    END IF;

    -- Если конец меньше начала (перескок на следующий день), прибавим день
    IF v_end_time <= v_start_time THEN
        v_end_time := v_end_time + interval '1 day';
    END IF;

    /*
      Логика:
      1) Выбираем упорядоченные строки за интервал.
      2) Помечаем строки, где litres > litres_prev + refuel_threshold => начало новой сессии (заправка).
      3) Нумеруем сегменты через суммирование флага заправки.
      4) Для каждого сегмента суммируем только положительные падения (и < max_step_drop).
    */
    WITH ordered AS (
      SELECT timecreated, liters, liters_prev,
             (liters_prev - liters) AS diff,
             CASE WHEN liters IS NOT NULL AND liters_prev IS NOT NULL AND (liters - liters_prev) > refuel_threshold THEN 1 ELSE 0 END AS is_refuel
      FROM public.mv_history_fuel_temp4
      --v_history_fuel_temp4
      WHERE eqmt = peqmt::text
        AND timecreated BETWEEN v_start_time AND v_end_time
      ORDER BY timecreated
    ),
    seg AS (
      SELECT *,
             SUM(is_refuel) OVER (ORDER BY timecreated ROWS UNBOUNDED PRECEDING) AS seg_no
      FROM ordered
    ),
    per_seg AS (
      SELECT seg_no,
             SUM(diff) FILTER (WHERE diff > 0 AND diff < max_step_drop) AS seg_consumption
      FROM seg
      GROUP BY seg_no
    )
    SELECT ROUND(COALESCE(SUM(seg_consumption),0)::numeric, 1)
    INTO v_fuel_used
    FROM per_seg;

    RETURN COALESCE(v_fuel_used, 0);
END;
$function$;




SELECT proname, oid::regprocedure, *
FROM pg_proc
WHERE proname ILIKE 'fuel_used_by_shifts_r1';

--DROP FUNCTION IF EXISTS fuel_used_by_shifts_r1
--DROP FUNCTION IF EXISTS public.fuel_used_by_shifts_r1(integer,date,integer,date,integer,numeric,numeric);




SELECT proname, oid::regprocedure, *
FROM pg_proc
WHERE proname ILIKE 'fuel_used_by_shifts_r181%';

--DROP FUNCTION IF EXISTS fuel_used_by_shifts_r181(integer,date,integer,date,integer);