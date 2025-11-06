CREATE OR REPLACE FUNCTION public.auxil_get_fuel_usage(timebegin integer, timeend integer)
 RETURNS TABLE(eqmtid text, fuel_used double precision, status text, accuracy numeric)
 LANGUAGE plpgsql
AS $function$
declare 
-- Период усреднения топливных значений в минутах
	agg_period integer:=20;
-- Максимальное изменение уровня топлива за agg_period
	max_fuel_diff integer:=70;
-- Минимальное изменение уровня топлива при заправке
	min_fueling_level integer:=250;
-- Процент неменяющихся записей для статуса - 'no data'
	perc_no_data float :=0.8;
-- Процент скачков топлива в записях для статуса - 'need calibration'
	perc_need_cab float :=0.1;

		begin
create temp table if not exists  temp_fuel_data (tm timestamp, eqmt text, fuel float,fuel_prev float);

insert into temp_fuel_data
select tm,eqmt::text,fuel,
lead(fuel,1) over (partition by eqmt order by tm desc) fuel_prev	
	from (
			SELECT date_trunc('hour',to_timestamp(time_created/1000)) + date_part('minute',to_timestamp(time_created/1000))::int/agg_period*interval '20 min' tm,eqmt,avg(liters) fuel
		 from history_fuel
		where time_created/1000 between timebegin and timeend
		group by date_trunc('hour',to_timestamp(time_created/1000)) + date_part('minute',to_timestamp(time_created/1000))::int/agg_period*interval '20 min',eqmt 
	) f1;

return query 
select eqmt,
case when perc_same >perc_no_data or perc_jump>perc_need_cab then null else fuel_use end fuel_used,
case when perc_same >perc_no_data then 'no_data'
		when perc_jump>perc_need_cab then 'need calibration'
			else 'Ok' end status,
round(100*case when perc_same >perc_no_data then null else perc_jump end,1) accuracy
from (
select eqmt,
sum(case when fuel_prev - fuel >0  and fuel_prev - fuel < max_fuel_diff then  fuel_prev - fuel else 0 end ) fuel_use,
sum(case when fuel_prev=fuel then 1.0 else 0.0 end)/count(*) perc_same,
sum(case when fuel_prev-fuel>max_fuel_diff or (fuel_prev-fuel<-max_fuel_diff and fuel_prev-fuel>-min_fueling_level)  then 1.0 else 0.0 end)/count(*) perc_jump
from temp_fuel_data 
where fuel >0 and fuel_prev>0 
group by eqmt
	) f2;

	drop table temp_fuel_data;
	end;
$function$
;

-- Permissions

ALTER FUNCTION public.auxil_get_fuel_usage(int4, int4) OWNER TO postgres;
GRANT ALL ON FUNCTION public.auxil_get_fuel_usage(int4, int4) TO postgres;
