-- 1. Stratificirano po deležu
-- opombe: table_name mora biti v nizu
CREATE OR REPLACE MACRO stratificirano_delezno(
    table_name,
    stratumi,
    delez,
    seme := 0.5
) AS TABLE
WITH osnova AS (
    SELECT *, stratumi AS _stratum
    FROM query_table(table_name)
),
stevila AS (
    SELECT _stratum, COUNT(*)::BIGINT AS c
    FROM osnova
    GROUP BY 1
),
razvrsceni AS (
    SELECT o.*,
           ROW_NUMBER() OVER (PARTITION BY _stratum ORDER BY random()) AS rn,
           s.c
    FROM osnova o
    JOIN stevila s USING (_stratum)
)
SELECT *
FROM razvrsceni
WHERE rn <= CEIL(c * delez);

-- 2) Stratificirano s fiksnim št. elementov na stratum (razlika od 1. metode je n_per)
CREATE OR REPLACE MACRO stratificirano_n(
    table_name,
    stratumi,
    n_per,
    seme := 0.5
) AS TABLE
WITH osnova AS (
    SELECT *, stratumi AS _stratum
    FROM query_table(table_name)
),
razvrsceni AS (
    SELECT o.*,
           ROW_NUMBER() OVER (PARTITION BY _stratum ORDER BY random()) AS rn
    FROM osnova o
)
SELECT *
FROM razvrsceni
WHERE rn <= n_per;

-- 3) Uteženo vzorčenje (z metodo/formulo za izračun A-ExpJ)
CREATE OR REPLACE MACRO utezeno(
    table_name,   -- npr. 'public.poslovni_subjekti'
    utez,         -- SQL izraz za utež
    n,
    seme := 0.5
) AS TABLE
SELECT *
FROM (
    SELECT t.*,
           (-ln(GREATEST(1e-12, random())) / NULLIF(utez, 0.0)) AS _kljuc
    FROM query_table(table_name) t
) v
WHERE _kljuc IS NOT NULL
ORDER BY _kljuc
LIMIT n;