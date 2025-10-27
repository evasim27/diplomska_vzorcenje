-- 1) Stratificirano vzorčenje z deležem

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

-- 2) Stratificirano vzorčenje s fiksnim številom elementov v stratumu

CREATE OR REPLACE PROCEDURE stratificirano_n(
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

-- 3) Uteženo vzorčenje

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

  PERFORM setseed(seme);

  EXECUTE format('DROP TABLE IF EXISTS %I', izhodna_tabela);

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

-- 4) Rezervoarsko vzorčenje

CREATE OR REPLACE PROCEDURE rezervoarsko(
  IN  tabela           regclass,
  IN  n                int,
  IN  seme             double precision DEFAULT NULL,
  IN  pogoj_sql        text            DEFAULT NULL,
  IN  izhodna_tabela   text            DEFAULT 'rezervoar_vzorec'
)
LANGUAGE plpgsql
AS $$
DECLARE
  ime_tabele   text := tabela::text;
  pogoj        text := CASE WHEN pogoj_sql IS NULL OR pogoj_sql = '' THEN '' ELSE ' WHERE '||pogoj_sql END;

  stevec       bigint := 0;
  nakljucni_ix bigint;
  rezervoar    tid[] := ARRAY[]::tid[];
  vrstica      record;
  poizvedba    text;
BEGIN
  IF n <= 0 THEN
    RAISE EXCEPTION 'n mora biti večji od 0!';
  END IF;

  IF seme IS NOT NULL THEN
    PERFORM setseed(seme);
  END IF;

  poizvedba := format('SELECT ctid FROM %s%s', ime_tabele, pogoj);

  FOR vrstica IN EXECUTE poizvedba LOOP
    stevec := stevec + 1;

    IF stevec <= n THEN
      rezervoar := array_append(rezervoar, vrstica.ctid);
    ELSE
      nakljucni_ix := floor(random() * stevec)::bigint + 1;
      IF nakljucni_ix <= n THEN
        rezervoar[nakljucni_ix] := vrstica.ctid;
      END IF;
    END IF;
  END LOOP;

  EXECUTE format('DROP TABLE IF EXISTS %I', izhodna_tabela);
  EXECUTE format($q$
    CREATE TEMP TABLE %I AS
    SELECT t.*
    FROM %s t
    JOIN unnest($1::tid[]) WITH ORDINALITY AS izbor(ctid, vrstni_red)
      ON t.ctid = izbor.ctid
    ORDER BY izbor.vrstni_red
  $q$, izhodna_tabela, ime_tabele)
  USING rezervoar;

  IF array_length(rezervoar, 1) IS DISTINCT FROM n THEN
    RAISE NOTICE 'V tabeli je bilo % vrstic; vzorec vsebuje % (manj od zahtevanih %).',
                 stevec, array_length(rezervoar, 1), n;
  END IF;
END $$;

-- 5) Enostopenjsko vzorčenje gruč

CREATE OR REPLACE PROCEDURE gruce_enostopenjsko(
  IN  tabela           regclass,
  IN  izraz_gruce      text,
  IN  stevilo_gruc     int,
  IN  seme             double precision DEFAULT 0.5,
  IN  pogoj_sql        text   DEFAULT NULL,
  IN  izhodna_tabela   text   DEFAULT 'vzorec_gruce1'
)
LANGUAGE plpgsql
AS $$
DECLARE
  ime_tabele text := tabela::text;
  pogoj      text := CASE WHEN pogoj_sql IS NULL OR pogoj_sql='' THEN '' ELSE 'WHERE '||pogoj_sql END;
  sql        text;
BEGIN
  IF stevilo_gruc <= 0 THEN
    RAISE EXCEPTION 'stevilo_gruc mora biti večje od 0!';
  END IF;

  PERFORM setseed(seme);
  EXECUTE format('DROP TABLE IF EXISTS %I', izhodna_tabela);

  sql := format($q$
    CREATE TEMP TABLE %I AS
    WITH osnova AS (
      SELECT t.*, (%s)::text AS _gruca
      FROM %s t
      %s
    ),
    unikatne_gruce AS (
      SELECT DISTINCT _gruca FROM osnova
    ),
    izbrane AS (
      SELECT _gruca
      FROM unikatne_gruce
      ORDER BY random()
      LIMIT %s
    )
    SELECT o.*
    FROM osnova o
    JOIN izbrane i USING (_gruca)
  $q$, izhodna_tabela, izraz_gruce, ime_tabele, pogoj, stevilo_gruc);

  EXECUTE sql;
END $$;

-- 6) Dvostopenjsko vzorčenje gruč

CREATE OR REPLACE PROCEDURE gruce_dvostopenjsko(
  IN  tabela           regclass,
  IN  izraz_gruce      text,
  IN  stevilo_gruc     int,
  IN  n_na_gruco       int,
  IN  seme             double precision DEFAULT 0.5,
  IN  pogoj_sql        text   DEFAULT NULL,
  IN  izhodna_tabela   text   DEFAULT 'vzorec_gruce2'
)
LANGUAGE plpgsql
AS $$
DECLARE
  ime_tabele text := tabela::text;
  pogoj      text := CASE WHEN pogoj_sql IS NULL OR pogoj_sql='' THEN '' ELSE 'WHERE '||pogoj_sql END;
  sql        text;
BEGIN
  IF stevilo_gruc <= 0 THEN
    RAISE EXCEPTION 'stevilo_gruc mora biti večje od 0!';
  END IF;
  IF n_na_gruco <= 0 THEN
    RAISE EXCEPTION 'n_na_gruco mora biti večje od 0!';
  END IF;

  PERFORM setseed(seme);
  EXECUTE format('DROP TABLE IF EXISTS %I', izhodna_tabela);

  sql := format($q$
    CREATE TEMP TABLE %I AS
    WITH osnova AS (
      SELECT t.*, (%s)::text AS _gruca
      FROM %s t
      %s
    ),
    unikatne_gruce AS (
      SELECT DISTINCT _gruca FROM osnova
    ),
    izbrane_gruce AS (
      SELECT _gruca
      FROM unikatne_gruce
      ORDER BY random()
      LIMIT %s
    ),
    oznacene AS (
      SELECT o.*,
             ROW_NUMBER() OVER (PARTITION BY o._gruca ORDER BY random()) AS rn
      FROM osnova o
      JOIN izbrane_gruce i USING (_gruca)
    )
    SELECT *
    FROM oznacene
    WHERE rn <= %s
  $q$, izhodna_tabela, izraz_gruce, ime_tabele, pogoj, stevilo_gruc, n_na_gruco);

  EXECUTE sql;
END $$;

-- 7) Sistematično vzorčenje

CREATE OR REPLACE PROCEDURE sistematicno_vzorcenje(
  IN  tabela           regclass,
  IN  urejanje_po      text,     -- npr. 'datum, id' ali 'some_score DESC'
  IN  korak            int,      -- vsak k-ti
  IN  zacetni_indeks   int DEFAULT 1,  -- 1 = začni z 1. zapisom, 2 = z 2. itd.
  IN  pogoj_sql        text DEFAULT NULL,
  IN  izhodna_tabela   text DEFAULT 'vzorec_sistematicno'
)
LANGUAGE plpgsql
AS $$
DECLARE
  ime_tabele text := tabela::text;
  pogoj      text := CASE WHEN pogoj_sql IS NULL OR pogoj_sql='' THEN '' ELSE 'WHERE '||pogoj_sql END;
  sql        text;
BEGIN
  IF korak <= 0 THEN
    RAISE EXCEPTION 'korak mora biti večji od 0!';
  END IF;
  IF zacetni_indeks <= 0 THEN
    RAISE EXCEPTION 'zacetni_indeks mora biti >= 1!';
  END IF;

  EXECUTE format('DROP TABLE IF EXISTS %I', izhodna_tabela);

  sql := format($q$
    CREATE TEMP TABLE %I AS
    WITH urejeno AS (
      SELECT *
      FROM %s t
      %s
      ORDER BY %s
    ),
    o AS (
      SELECT u.*, ROW_NUMBER() OVER () AS rn
      FROM urejeno u
    )
    SELECT *
    FROM o
    WHERE rn >= %s
      AND ((rn - %s) %% %s) = 0
  $q$, izhodna_tabela, ime_tabele, pogoj, urejanje_po, zacetni_indeks, zacetni_indeks, korak);

  EXECUTE sql;
END $$;