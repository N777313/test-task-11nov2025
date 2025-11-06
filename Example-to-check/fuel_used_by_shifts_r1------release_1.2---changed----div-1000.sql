SELECT fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-21',
    pendshift := 1
);



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
    WHERE eqmt = peqmt::text  -- 🔹 Приведение типов
      AND timecreated BETWEEN v_start_time AND v_end_time;

    -- 4️⃣ Возврат результата
    RETURN COALESCE(v_fuel_used, 0);
END;
$function$;
