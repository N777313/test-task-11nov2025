CREATE OR REPLACE FUNCTION public.fuel_consumption(peqmt integer, pstartdate date, pstartshift integer, penddate date, pendshift integer)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    start_ts TIMESTAMP;
    end_ts TIMESTAMP;
    end_shift_start_ts TIMESTAMP;
    total_consumption NUMERIC := 0;

    -- Конечный автомат для сложного расчета потребления
    state TEXT := 'CONSUMING'; -- Состояния: CONSUMING (Потребление), POTENTIAL_REFILL (Возможная заправка), FINDING_NEW_PEAK (Поиск нового пика)
    liters_cursor CURSOR FOR
        SELECT liters FROM history_fuel
        WHERE eqmt = peqmt
          AND to_timestamp(time_created / 1000.0) >= start_ts
          AND to_timestamp(time_created / 1000.0) < end_ts
        ORDER BY time_created ASC;
    
    current_liters NUMERIC;
    last_liters NUMERIC;

    -- Переменные для отслеживания сегментов потребления
    start_of_segment_liters NUMERIC;
    min_in_segment_liters NUMERIC;

    -- Переменные для отслеживания и подтверждения заправок
    increase_sum NUMERIC;
    REFILL_CONFIRM_THRESHOLD NUMERIC := 100;

BEGIN
    -- Валидация входных данных: проверка, что начало <= конец
    IF pstartdate > penddate OR (pstartdate = penddate AND pstartshift > pendshift) THEN
        RAISE EXCEPTION 'Invalid shift interval: start must be before or equal to end';
    END IF;

    -- Получение временной метки начала стартовой смены
    SELECT shiftstart INTO start_ts
    FROM shifts
    WHERE shiftdate = pstartdate AND shift = pstartshift;
    IF start_ts IS NULL THEN
        RAISE NOTICE 'Starting shift not found for date % and shift %, returning 0', pstartdate, pstartshift;
        RETURN 0;
    END IF;

    -- Получение временной метки начала конечной смены
    SELECT shiftstart INTO end_shift_start_ts
    FROM shifts
    WHERE shiftdate = penddate AND shift = pendshift;
    IF end_shift_start_ts IS NULL THEN
        RAISE NOTICE 'Ending shift not found for date % and shift %, returning 0', penddate, pendshift;
        RETURN 0;
    END IF;

    -- Поиск временной метки начала следующей смены после конечной
    SELECT MIN(shiftstart) INTO end_ts
    FROM shifts
    WHERE shiftstart > end_shift_start_ts;

    -- Если нет следующей смены, использовать максимальное time_created из history_fuel как запасной вариант
    IF end_ts IS NULL THEN
        RAISE NOTICE 'No next shift found after ending shift, using max time_created from history_fuel as fallback';
        SELECT to_timestamp(MAX(time_created) / 1000.0) INTO end_ts
        FROM history_fuel
        WHERE eqmt = peqmt;
        IF end_ts IS NULL THEN
            RAISE NOTICE 'No fuel data available for eqmt %', peqmt;
            RETURN 0;
        END IF;
    END IF;

    -- Инициализация состояния по первой точке данных
    OPEN liters_cursor;
    FETCH liters_cursor INTO last_liters;

    IF NOT FOUND THEN
        RAISE NOTICE 'No fuel readings found for eqmt % in the interval [% - %)', peqmt, start_ts, end_ts;
        CLOSE liters_cursor;
        RETURN 0;
    END IF;

    start_of_segment_liters := last_liters;
    min_in_segment_liters := last_liters;

    -- Обработка всех последующих показаний уровня топлива
    LOOP
        FETCH liters_cursor INTO current_liters;
        EXIT WHEN NOT FOUND;

        IF state = 'CONSUMING' THEN -- Если мы в состоянии потребления
            IF current_liters < min_in_segment_liters THEN
                min_in_segment_liters := current_liters;
            ELSIF current_liters > last_liters THEN -- Если уровень топлива увеличился, то мы нашли потенциальную заправку
                state := 'POTENTIAL_REFILL';
                increase_sum := current_liters - last_liters;
            END IF;
        
        ELSIF state = 'POTENTIAL_REFILL' THEN -- Если мы в состоянии потенциальной заправки
            IF current_liters > last_liters THEN
                increase_sum := increase_sum + (current_liters - last_liters);

                IF increase_sum >= REFILL_CONFIRM_THRESHOLD THEN
                    IF start_of_segment_liters > min_in_segment_liters THEN -- Если уровень топлива увеличился, то мы нашли потенциальную заправку
                         total_consumption := total_consumption + (start_of_segment_liters - min_in_segment_liters);
                    END IF;
                    state := 'FINDING_NEW_PEAK'; 
                END IF;
            ELSIF current_liters < last_liters THEN
                state := 'CONSUMING';
                min_in_segment_liters := LEAST(min_in_segment_liters, current_liters);
            END IF;

        ELSIF state = 'FINDING_NEW_PEAK' THEN -- Если мы в состоянии поиска нового пика
            IF current_liters < last_liters THEN -- Если уровень топлива уменьшился, то мы нашли новый пик
                state := 'CONSUMING';
                start_of_segment_liters := last_liters;
                min_in_segment_liters := current_liters;
            END IF;
        END IF;

        last_liters := current_liters;
    END LOOP;

    CLOSE liters_cursor;

    -- Добавление расхода из последнего сегмента, если это не была заправка
    IF state = 'CONSUMING' OR state = 'POTENTIAL_REFILL' THEN
        IF start_of_segment_liters > min_in_segment_liters THEN
             total_consumption := total_consumption + (start_of_segment_liters - min_in_segment_liters);
        END IF;
    END IF;

    IF total_consumption = 0 THEN
        RAISE NOTICE 'No fuel consumption detected for eqmt % in the interval (possible refills only or no usage)', peqmt;
    END IF;

    RETURN total_consumption;
END;
$function$
;

-- Permissions

ALTER FUNCTION public.fuel_consumption(int4, date, int4, date, int4) OWNER TO stu;
GRANT ALL ON FUNCTION public.fuel_consumption(int4, date, int4, date, int4) TO stu;
