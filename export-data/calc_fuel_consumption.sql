CREATE OR REPLACE FUNCTION public.calc_fuel_consumption(peqmt integer, pstartdate date, pstartshift integer, penddate date, pendshift integer)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    start_ts BIGINT;
    end_ts BIGINT;
    start_fuel NUMERIC;
    end_fuel NUMERIC;
    consumption NUMERIC;
BEGIN

    SELECT EXTRACT(EPOCH FROM shiftstart) * 1000
    INTO start_ts
    FROM shifts
    WHERE shiftdate = pstartdate
        AND shift = pstartshift
    LIMIT 1;

    IF start_ts IS NULL THEN
        --RAISE EXCEPTION 'Cannot find StartTs: EQMT %, Date %, Shift %', peqmt, pstartdate, pstartshift;
    	RETURN 0;
	END IF;

    SELECT EXTRACT(EPOCH FROM shiftstart) * 1000
    INTO end_ts
    FROM shifts
    WHERE
        (shiftdate > penddate)
        OR (shiftdate = penddate AND shift > pendshift)
    ORDER BY
        shiftdate ASC,
        shift ASC
    LIMIT 1;

    IF end_ts IS NULL THEN
        SELECT MAX(time_created)
        INTO end_ts
        FROM history_fuel
        WHERE
            eqmt = peqmt::INTEGER  
            AND time_created >= start_ts;
    END IF;

    IF end_ts IS NULL THEN
        RAISE EXCEPTION 'Cannot find info for % within the interval.', peqmt;
    END IF;

    SELECT liters
    INTO start_fuel
    FROM history_fuel
    WHERE
        eqmt = peqmt::INTEGER  
        AND time_created <= start_ts
    ORDER BY
        time_created DESC
    LIMIT 1;

    -- 4. GET END FUEL LEVEL
    SELECT liters
    INTO end_fuel
    FROM history_fuel
    WHERE
        eqmt = peqmt::INTEGER  
        AND time_created <= end_ts
    ORDER BY
        time_created DESC
    LIMIT 1;

    IF start_fuel IS NULL OR end_fuel IS NULL THEN
        RAISE EXCEPTION 'StartFuel or endFuel is null for EQMT % in the interval.', peqmt;
    END IF;


    consumption := start_fuel - end_fuel;

    RETURN consumption;

END;
$function$
;

-- Permissions

ALTER FUNCTION public.calc_fuel_consumption(int4, date, int4, date, int4) OWNER TO stu;
GRANT ALL ON FUNCTION public.calc_fuel_consumption(int4, date, int4, date, int4) TO stu;
