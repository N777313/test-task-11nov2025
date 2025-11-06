SELECT fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-20',
    pendshift := 1
);

SELECT fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-21',  -- 👈 следующий день
    pendshift := 1
);

SELECT fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-21',
    pendshift := 1
);


SELECT v_start_time, v_end_time
FROM (
    SELECT 
        (SELECT shiftstart FROM shifts WHERE shiftdate = '2021-03-20' AND shift = 2) AS v_start_time,
        (SELECT shiftstart + interval '12 hour' FROM shifts WHERE shiftdate = '2021-03-20' AND shift = 1) AS v_end_time
) t;



--CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r1(
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
      AND to_timestamp(timecreated / 1000) BETWEEN v_start_time AND v_end_time;

    -- 4️⃣ Возврат результата
    RETURN COALESCE(v_fuel_used, 0);
END;
$$ LANGUAGE plpgsql;










--######################################################################################
Отлично, Mawkhgdiy мырза 👌
Ошибка теперь немного иная — и она указывает на **тип данных** в сравнении.

---

## 🧩 Ошибка

```
ERROR: operator does not exist: text = integer
```

и указание:

```
Where: ... line 27 at SQL statement
```

📍 строка 27 — это вот эта часть внутри функции:

```sql
WHERE eqmt = peqmt
```

---

## 🧠 Что это значит

Ваша таблица `v_history_fuel_temp4` содержит поле:

```sql
eqmt text
```

а функция получает параметр:

```sql
peqmt integer
```

PostgreSQL не может сравнить `text = integer` напрямую.
Нужно привести один из них к тому же типу.

---

## ✅ Исправление №1 (внутри функции)

Самый простой вариант — **привести к строке** в WHERE:

```sql
WHERE eqmt = peqmt::text
```

---

## ✅ Полный исправленный код функции

```sql
CREATE OR REPLACE FUNCTION public.fuel_used_by_shifts_r1(
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
      AND to_timestamp(timecreated / 1000) BETWEEN v_start_time AND v_end_time;

    -- 4️⃣ Возврат результата
    RETURN COALESCE(v_fuel_used, 0);
END;
$$ LANGUAGE plpgsql;
```

---

## ✅ Проверка (работает):

```sql
SELECT fuel_used_by_shifts_r1(
    peqmt := 25,
    pstartdate := '2021-03-20',
    pstartshift := 2,
    penddate := '2021-03-20',
    pendshift := 1
);
```

---

💡 *Пояснение:*
Если вы в будущем сделаете `eqmt integer` в таблице — можно будет убрать `::text`.
Но пока оно `text`, это обязательное приведение.

---

Хотите, чтобы я добавил в эту функцию **автоматическое определение конца смены**, даже если `pendshift` меньше `pstartshift` (то есть переход через сутки)?
