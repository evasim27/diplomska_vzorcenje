-- opomba: stevilo_na_stratum mora biti celo št.
CREATE OR REPLACE PROCEDURE stratificirano_n_random(
  IN  tabela            regclass,
  IN  stratumi          text[],
  IN  stevilo_na_stratum int,
  IN  seme              double precision DEFAULT 0.5,
  IN  izhodna_tabela    text   DEFAULT 'vzorec_stratumi_n',
  IN  pogoj_sql         text   DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
  ime_tabele    text := tabela::text;
  izraz_stratum text;
  pogoj         text := CASE WHEN pogoj_sql IS NULL OR pogoj_sql='' THEN '' ELSE 'WHERE '||pogoj_sql END;
  poizvedba     text;
BEGIN
  IF stevilo_na_stratum <= 0 THEN
    RAISE EXCEPTION 'stevilo_na_stratum mora biti večje od 0!';
  END IF;

--seme
  PERFORM setseed(seme);

  SELECT '(' || string_agg(format('COALESCE(%1$I::text,''∅'')', c), '||'':''||') || ')'
  INTO izraz_stratum
  FROM unnest(stratumi) AS c;

  EXECUTE format('DROP TABLE IF EXISTS %I', izhodna_tabela);

  poizvedba := format($f$
    CREATE TEMP TABLE %I AS
    WITH osnova AS (
      SELECT t.*, %s AS _stratum
      FROM %s t
      %s
    ),
    razvrsceni AS (
      SELECT b.*,
             row_number() OVER (PARTITION BY _stratum ORDER BY random()) AS rn
      FROM osnova b
    )
    SELECT * FROM razvrsceni
    WHERE rn <= %s
  $f$, izhodna_tabela, izraz_stratum, ime_tabele, pogoj, stevilo_na_stratum);

  EXECUTE poizvedba;
END $$;
