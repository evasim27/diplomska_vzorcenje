--opomba: delez je med 0 in 1
CREATE OR REPLACE PROCEDURE stratificirano_delezno(
  IN  tabela           regclass,
  IN  stratumi         text[],
  IN  delez            double precision,
  IN  seme             double precision DEFAULT 0.5,
  IN  izhodna_tabela   text   DEFAULT 'vzorec_stratumi_delez',
  IN  pogoj_sql        text   DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
  ime_tabele    text := tabela::text;
  izraz_stratum text;
  pogoj         text := CASE WHEN pogoj_sql IS NULL OR pogoj_sql='' THEN '' ELSE 'WHERE '||pogoj_sql END;
  poizvedba     text;
BEGIN
  IF delez < 0 OR delez > 1 THEN
    RAISE EXCEPTION 'Delež mora biti med 0 in 1!';
  END IF;

-- seme
  PERFORM setseed(seme);

  SELECT '(' || string_agg(format('COALESCE(%1$I::text,''∅'')', c), '||'':''||') || ')'
  INTO izraz_stratum
  FROM unnest(stratumi) AS c;

  EXECUTE format('DROP TABLE IF EXISTS %I', izhodna_tabela);

  poizvedba := format($f$
    CREATE TEMP TABLE %I AS
    WITH osnova AS (
      SELECT t.*,
             %s AS _stratum
      FROM %s t
      %s
    ),
    razvrsceni AS (
      SELECT o.*,
             COUNT(*) OVER (PARTITION BY _stratum)                                 AS c,
             ROW_NUMBER() OVER (PARTITION BY _stratum ORDER BY random())          AS rn
      FROM osnova o
    )
    SELECT *
    FROM razvrsceni
    WHERE rn <= CEIL(c * %s)
  $f$, izhodna_tabela, izraz_stratum, ime_tabele, pogoj, delez::text);

  EXECUTE poizvedba;
END $$;
