CREATE OR REPLACE PROCEDURE utezeno(
  IN  tabela          regclass,
  IN  utez_izraz      text,
  IN  n               int,
  IN  seme            double precision DEFAULT 0.5,
  IN  pogoj_sql       text   DEFAULT NULL,
  IN  izhodna_tabela  text   DEFAULT 'vzorec_utezeno'
)
LANGUAGE plpgsql
AS $$
DECLARE
  ime_tabele text := tabela::text;
  pogoj      text := CASE WHEN pogoj_sql IS NULL OR pogoj_sql=''
                          THEN ''
                          ELSE 'WHERE '||pogoj_sql END;
  sql        text;
BEGIN
  IF n <= 0 THEN
    RAISE EXCEPTION 'n mora biti večji od 0!';
  END IF;

--seme
  PERFORM setseed(seme);

  EXECUTE format('DROP TABLE IF EXISTS %I', izhodna_tabela);

-- metoda AExp-J
  sql := format($q$
    CREATE TEMP TABLE %I AS
    SELECT *
    FROM (
      SELECT t.*,
             (-ln(GREATEST(1e-12, random())) / (%s)::double precision) AS _kljuc
      FROM %s t
      %s
    ) s
    WHERE _kljuc IS NOT NULL
    ORDER BY _kljuc
    LIMIT %s
  $q$, izhodna_tabela, utez_izraz, ime_tabele, pogoj, n);

  EXECUTE sql;
END $$;
