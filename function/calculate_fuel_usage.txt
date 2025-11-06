CREATE OR REPLACE FUNCTION public.calculate_fuel_usage(p_eqmt integer, p_start_date date, p_start_shift integer, p_end_date date, p_end_shift integer)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    start_period TIMESTAMPTZ;
    end_period   TIMESTAMPTZ;
    total NUMERIC;
BEGIN
    SELECT shiftstart
  	INTO start_period
  	FROM shifts
    WHERE shiftdate = p_start_date
   	AND shift = p_start_shift
	LIMIT 1;

    SELECT shiftstart
    INTO end_period
    FROM shifts
    WHERE shiftstart > (
        SELECT shiftstart
        FROM shifts
        WHERE shiftdate = p_end_date
        AND shift = p_end_shift
        LIMIT 1
    )
    ORDER BY shiftstart
    LIMIT 1;

	IF start_period IS NULL OR end_period IS NULL THEN
        RETURN 0;
    END IF;

    CREATE TEMP TABLE tmp_fuel AS
    SELECT 
        to_timestamp(time_created/1000.0) AS ts,
        liters
    FROM history_fuel
    WHERE eqmt = p_eqmt
  	AND to_timestamp(time_created/1000.0) BETWEEN start_period AND end_period
  	AND liters IS NOT NULL
    ORDER BY ts;

    CREATE TEMP TABLE tmp_segments AS
    SELECT
        ts,
        liters,
        LAG(liters) OVER (ORDER BY ts) AS prev_liters,
        liters - LAG(liters) OVER (ORDER BY ts) AS diff
    FROM tmp_fuel;

    SELECT SUM(abs(diff)) INTO total
    FROM tmp_segments
    WHERE diff < 0;

    DROP TABLE IF EXISTS tmp_fuel;
    DROP TABLE IF EXISTS tmp_segments;

    RETURN COALESCE(total, 0);
END;
$function$
;
