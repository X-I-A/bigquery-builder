/*
Base Test Cases
 */
WITH
  Base AS (
  /*
  Word Account
   */
    SELECT DISTINCT
      mtart,
      matnr,
      CURRENT_TIMESTAMP () as budat
    FROM
      `test_builder.mara` AS a
        INNER JOIN
      `test_builder.mara` AS b
        USING (matnr, mtart)
  ),
  Semantics AS (
    SELECT
      mtart,
      COUNT(matnr) as matnr,
      MAX(budat) as budat
    FROM
      Base
    GROUP BY
      mtart
  )
  SELECT * From Semantics
