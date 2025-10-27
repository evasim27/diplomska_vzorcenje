-- 1. Stratificirano vzorčenje po deležu

CREATE OR REPLACE MACRO stratificirano_delezno(
    ime_tabele,
    stratumi,
    delez,
    seme := 0.5
) AS TABLE
WITH osnova AS (
    SELECT *, stratumi AS _stratum
    FROM query_table(ime_tabele)
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

-- 2) Stratificirano s fiksnim št. elementov na stratum

CREATE OR REPLACE MACRO stratificirano_n(
    ime_tabele,
    stratumi,
    n_per,
    seme := 0.5
) AS TABLE
WITH osnova AS (
    SELECT *, stratumi AS _stratum
    FROM query_table(ime_tabele)
),
razvrsceni AS (
    SELECT o.*,
           ROW_NUMBER() OVER (PARTITION BY _stratum ORDER BY random()) AS rn
    FROM osnova o
)
SELECT *
FROM razvrsceni
WHERE rn <= n_per;

-- 3) Uteženo vzorčenje

CREATE OR REPLACE MACRO utezeno(
    ime_tabele,
    utez,
    n,
    seme := 0.5
) AS TABLE
SELECT *
FROM (
    SELECT t.*,
           (-ln(GREATEST(1e-12, random())) / NULLIF(utez, 0.0)) AS _kljuc
    FROM query_table(ime_tabele) t
) v
WHERE _kljuc IS NOT NULL
ORDER BY _kljuc
LIMIT n;

-- 4) Vzorčenje gruč (enostopenjsko)

CREATE OR REPLACE MACRO vzorcenje_gruc_enostopenjsko(
    ime_tabele,
    izraz_gruce,
    stevilo_gruc
) AS TABLE
WITH osnova AS (
    SELECT *, izraz_gruce AS _gruca
    FROM query_table(ime_tabele)
),
izbrane_gruce AS (
    SELECT DISTINCT _gruca
    FROM osnova
    ORDER BY random()
    LIMIT stevilo_gruc
)
SELECT o.*
FROM osnova o
JOIN izbrane_gruce g USING (_gruca);

--5) Vzorčenje gruč (dvostopenjsko)

CREATE OR REPLACE MACRO vzorcenje_gruc_dvostopenjsko(
    ime_tabele,
    izraz_gruce,
    stevilo_gruc,
    stevilo_na_gruco
) AS TABLE
WITH osnova AS (
    SELECT *, izraz_gruce AS _gruca
    FROM query_table(ime_tabele)
),
izbrane_gruce AS (
    SELECT DISTINCT _gruca
    FROM osnova
    ORDER BY random()
    LIMIT stevilo_gruc
),
oznake AS (
    SELECT o.*,
           ROW_NUMBER() OVER (PARTITION BY o._gruca ORDER BY random()) AS rn
    FROM osnova o
    JOIN izbrane_gruce g USING (_gruca)
)
SELECT *
FROM oznake
WHERE rn <= stevilo_na_gruco;

--6) Sistematično vzorčenje

CREATE OR REPLACE MACRO vzorcenje_sistematicno(
    ime_tabele,
    izraz_urejanja,
    korak,
    zacetni_indeks
) AS TABLE
WITH p AS (
  SELECT
    CASE WHEN korak < 1 THEN 1 ELSE korak END AS k,
    CASE WHEN zacetni_indeks < 1 THEN 1 ELSE zacetni_indeks END AS s
),
osnova AS (
  SELECT *
  FROM query_table(ime_tabele)
  ORDER BY izraz_urejanja
),
oznacevanje AS (
  SELECT o.*,
         ROW_NUMBER() OVER () AS rn,
         p.s, p.k
  FROM osnova o, p
)
SELECT *
FROM oznacevanje
WHERE ((rn - s) % k) = 0;
