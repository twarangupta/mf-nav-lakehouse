-- Tile 4: risk vs return scatter. X = 1Y volatility, Y = 3Y CAGR, colour = asset class.
SELECT
  split(category_group, ' - ')[0]        AS `Asset class`,
  category_group                         AS `Category`,
  plan                                   AS `Plan`,
  scheme_name                            AS `Fund`,
  round(volatility_1y * 100, 2)          AS `Volatility 1Y %`,
  round(cagr_3y * 100, 2)                AS `3Y CAGR %`
FROM mf_nav.gold.fund_metrics
WHERE is_active AND cagr_3y IS NOT NULL AND volatility_1y IS NOT NULL
