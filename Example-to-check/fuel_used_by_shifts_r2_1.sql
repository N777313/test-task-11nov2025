--Example to check data

/*
CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r2_1(
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
    v_end_time   timestamp;
    v_fuel_used  numeric;
BEGIN
    -- начало первой смены
    SELECT shiftstart
    INTO v_start_time
    FROM shifts
    WHERE shiftdate = pstartdate AND shift = pstartshift;

    -- попытка найти начало конечной смены
    SELECT shiftstart
    INTO v_end_time
    FROM shifts
    WHERE shiftdate = penddate AND shift = pendshift;

    -- если не нашли — взять начало ближайшей следующей смены
    IF v_end_time IS NULL THEN
        SELECT MIN(shiftstart)
        INTO v_end_time
        FROM shifts
        WHERE shiftstart > v_start_time;
    END IF;

    -- если всё ещё нет — просто +12 часов
    IF v_end_time IS NULL THEN
        v_end_time := v_start_time + interval '12 hour';
    END IF;

    -- расчёт расхода топлива
    SELECT ROUND(SUM(liters_prev - liters)
                 FILTER (WHERE liters_prev > liters), 1)
    INTO v_fuel_used
    FROM v_history_fuel_temp4
    WHERE eqmt = peqmt
      AND to_timestamp(timecreated / 1000)
          BETWEEN v_start_time AND v_end_time;

    RETURN COALESCE(v_fuel_used, 0);
END;
$$ LANGUAGE plpgsql;
*/

SELECT fuel_used_by_shifts_r2_1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-20',
    pendshift := 1
);
