--public.fuel_consumption
--4 function
SELECT public.fuel_consumption(
    123,             -- ID оборудования (eqmt)
    '2025-11-01',    -- дата начала
    1,               -- номер смены начала
    '2025-11-03',    -- дата конца
    2                -- номер смены конца
);
--No fuel readings found for eqmt 123 in the interval [2025-10-31 20:45:00 - 2025-11-03 20:45:00)
--No fuel consumption detected for eqmt 25 in the interval (possible refills only or no usage)
--No fuel readings found for eqmt 123 in the interval [2025-10-31 20:45:00 - 2025-11-03 20:45:00)

--4 function 
SELECT public.fuel_consumption(
    peqmt := 25,
    pstartdate := '2021-02-10',
    pstartshift := 1,
    penddate := '2021-02-11',
    pendshift := 2
);
--fuel_consumption:0

--3 function
SELECT public.calculate_fuel_usage(
    25,             -- ID оборудования (eqmt)
    '2021-02-10',    -- дата начала
    1,               -- номер смены начала
    '2021-02-11',    -- дата конца
    2                -- номер смены конца
);
--calculate_fuel_usage:128

--2 function
SELECT public.calc_fuel_consumption(
    25,             -- ID оборудования (eqmt)
    '2021-02-10',    -- дата начала
    1,               -- номер смены начала
    '2021-02-11',    -- дата конца
    2                -- номер смены конца
);
--calc_fuel_consumption:-1298


--1) function
SELECT public.analyze_fuel_consumption_by_bekzat(
    25,             -- ID оборудования (eqmt)
    '2021-02-10',    -- дата начала
    1,               -- номер смены начала
    '2021-02-11',    -- дата конца
    2                -- номер смены конца
);

--total_consumption_liters	total_refilled_liters	refills_count	anomalous_drops_count	avg_consumption_lph	start_fuel_level	end_fuel_level	data_points_processed
--92.0	1833.0	1	0	1.9166666666666667	0.0	1741.0	779


SELECT public.calc_fuel_consumption(
    43,             -- ID оборудования (eqmt)
    '2020-04-19',    -- дата начала
    2,               -- номер смены начала
    '2020-04-19',    -- дата конца
    1                -- номер смены конца
);

select * from shifts s order by 1,2;
select distinct eqmt  from history_fuel hf order by 1;


SELECT version();
