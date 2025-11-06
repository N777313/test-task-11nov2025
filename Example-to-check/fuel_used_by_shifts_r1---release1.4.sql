/*
1) Что сделать прямо сейчас — остановить зависший запрос и посмотреть, что выполняется
Выполните (как суперпользователь или владелец БД):
Показать активные запросы и найти PID зависшего:
*/
-- посмотреть активные запросы
SELECT pid, usename, query_start, state, wait_event, query
FROM pg_stat_activity
WHERE state <> 'idle'
ORDER BY query_start;


-- отменить запрос
SELECT pg_cancel_backend(139);
-- или при необходимости убить сессию:
SELECT pg_terminate_backend(<pid>);





SELECT fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-20',
    pendshift := 1
);


--executed by Nurlan
--CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r1(
    peqmt integer,
    pstartdate date,
    pstartshift integer,
    penddate date,
    pendshift integer
)
RETURNS numeric
LANGUAGE plpgsql
AS $function$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_fuel_used numeric;
BEGIN
    -- 1️⃣ Время начала первой смены
    SELECT shiftstart
    INTO v_start_time
    FROM shifts
    WHERE shiftdate = pstartdate
      AND shift = pstartshift;

    -- 2️⃣ Время конца последней смены (+12 часов)
    SELECT shiftstart + interval '12 hour'
    INTO v_end_time
    FROM shifts
    WHERE shiftdate = penddate
      AND shift = pendshift;

    IF v_start_time IS NULL OR v_end_time IS NULL THEN
        RAISE EXCEPTION 'Не найдены границы смен (start: %, end: %)', v_start_time, v_end_time;
    END IF;

    -- 3️⃣ Расчёт расхода топлива без заправок и шумов
    SELECT 
        ROUND(SUM(diff) , 1)
    INTO v_fuel_used
    FROM (
        SELECT 
            liters_prev - liters AS diff
        FROM v_history_fuel_temp4
        WHERE eqmt = peqmt::text
          AND to_timestamp(timecreated / 1000) BETWEEN v_start_time AND v_end_time
          AND liters_prev IS NOT NULL
          AND liters IS NOT NULL
          -- 🔹 учитываем только падения уровня (расход)
          AND liters_prev > liters
          -- 🔹 отсекаем нереальные скачки (например, > 200 л за 10 мин)
          AND (liters_prev - liters) < 200
    ) t;

    RETURN COALESCE(v_fuel_used, 0);
END;
$function$;





