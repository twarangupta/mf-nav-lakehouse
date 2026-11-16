-- Tile 1: headline numbers (counter tiles). One row.
SELECT
  as_of_date                AS `Latest NAV date`,
  active_growth_schemes     AS `Active Growth schemes`,
  schemes_with_3y_cagr      AS `Schemes with 3Y CAGR`,
  silver_nav_rows           AS `Clean NAV records`,
  'Data: AMFI'              AS `Source`
FROM mf_nav.gold.kpis
