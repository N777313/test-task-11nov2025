-- public.history_fuel definition

-- Drop table

-- DROP TABLE public.history_fuel;

CREATE TABLE public.history_fuel (
	time_created int8 NULL,
	eqmt int4 NULL,
	raw varchar NULL,
	liters float8 NULL
);
CREATE INDEX "fuel time and eqmt" ON public.history_fuel USING btree (time_created, eqmt);

-- Permissions

--ALTER TABLE public.history_fuel OWNER TO auxil;


--#############################	NEXT table
-- public.shifts definition

-- Drop table

-- DROP TABLE public.shifts;

CREATE TABLE public.shifts (
	shiftstart timestamp NOT NULL,
	shiftdate date NULL,
	shift int4 NULL,
	crew int4 NULL,
	CONSTRAINT shifts_pkey PRIMARY KEY (shiftstart)
);
CREATE INDEX shiftstart ON public.shifts USING btree (shiftstart);

-- Permissions

ALTER TABLE public.shifts OWNER TO auxil;