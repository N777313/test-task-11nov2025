
-- Good example of to check the data
select * from mv_history_fuel_temp4 
WHERE 
	eqmt = '25'
  	AND timecreated BETWEEN TIMESTAMP '2021-03-20 08:45:00' AND TIMESTAMP '2021-03-20 20:45:00' order by 1;

select * from shifts where shiftstart::date >= '2021-03-20' order by 1;
--2021-03-20 08:45:00' AND TIMESTAMP '2021-03-20 20:45:00' order by 1;
--############################################################################




--№№№№№
SELECT shiftdate, shift, shiftstart
FROM shifts
WHERE shiftdate = '2021-03-20';
