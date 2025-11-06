CREATE OR REPLACE FUNCTION public.auxil_get_enghrs_hist(eqmtid text, timestart integer, timeend integer)
 RETURNS double precision
 LANGUAGE plpgsql
AS $function$
declare 

		enghrs float;
		
		begin
--Временная таблица со всеми внесенными операторами значениями --
	create temp table if not exists temp_enghrs_1 (time_created1 bigint,eqmt1 integer,engrs1 integer);
	insert into temp_enghrs_1
	select *
	from public.history_enghrs 
	where eqmt::text=eqmtid and engrs>0 and time_created/1000 between timestart and timeend;
-- Временная таблица с отфильтрованными некорректными значениями 
	create temp table if not exists temp_enghrs_2 (time_created2 bigint,eqmt2 integer,engrs2 integer,prv integer,nxt integer,prv_time bigint,rown_desc integer,rown_asc integer);
	insert into temp_enghrs_2
		select *,
			row_number()  over (partition by eqmt1 order by time_created1 desc) rown_desc,
			row_number()  over (partition by eqmt1 order by time_created1 asc) rown_asc
	from (
		select *,
		lead(engrs1,1) over (partition by eqmt1 order by time_created1 desc) prv,
		lag(engrs1,1) over (partition by eqmt1 order by time_created1 desc) nxt,
		lead(time_created1,1) over (partition by eqmt1 order by time_created1 desc) prv_time
		from temp_enghrs_1
	) m1
	where  coalesce(engrs1-prv,0)> 0 and coalesce(engrs1-prv,0) <= (time_created1-prv_time)/1000.0/3600.0+1;
	
	IF (select count(*) temp_enghrs_2) >1 then 
--Если есть более одного адекватного значения,внесенного оператором
	enghrs:=(select 
-- Суммируем 3 составляющих моточасов
			round(hrs_man_input+hrs_status_last+hrs_status_first,1) hours_total
				from (
				select 
-- 1) Берем разницу между макс и мин внесенных операторами за период
				eqmt2, max(engrs2) - min(engrs2) hrs_man_input,
-- 2) Находим рабочее время (готов и задержка) от начала периода до первого внесения оператором
				(select
					timeend -
					  time_created2/1000::integer 
				 -
-- отнимаем время в поломке и ожидании за период
					coalesce(public.auxil_get_idles_bystatus(
					eqmt2::text, 
					time_created2/1000::integer,
					timeend::integer,
					1,0,3,0),0)
				 from temp_enghrs_2 two where two.rown_desc=1 and two.eqmt2::text=eqmtid
					)/3600.0 hrs_status_last,
-- 3)Находим рабочее время (готов и задержка) от последнего внесения оператором до конца периода
				(select
					time_created2/1000::integer -
					timestart -
					coalesce(public.auxil_get_idles_bystatus(
					eqmt2::text, 
					timestart::integer,
					time_created2/1000::integer,
					1,0,3,0),0)
					from temp_enghrs_2 two where  two.rown_asc=1 and two.eqmt2::text=eqmtid)/3600.0 hrs_status_first

				from temp_enghrs_2 main 
				group by main.eqmt2
				) m2);
		ELSE
--Если нет данных по внесению оператором, считаем рабочее время (готов + задержка) за моточасы
		enghrs:=(select
					timeend -
					timestart -
					coalesce(public.auxil_get_idles_bystatus(
					eqmtid::text, 
					timestart::integer,
					timeend::integer,
					1,0,3,0),0) )/3600.0;
		END IF;

	drop table temp_enghrs_1;
	drop table temp_enghrs_2;
	return enghrs;
	end;
$function$
;
