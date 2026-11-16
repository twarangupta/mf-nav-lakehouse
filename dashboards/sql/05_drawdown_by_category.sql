-- Tile 5: median 3-year maximum drawdown by category (bar chart), Direct plans,
-- categories with at least 5 funds that have a 3-year history.
SELECT
  category_group                            AS `Category`,
  round(median_max_drawdown_3y * 100, 2)    AS `Median max drawdown 3Y %`,
  round(median_cagr_3y * 100, 2)            AS `Median 3Y CAGR %`,
  schemes_with_3y                           AS `Funds with 3Y history`
FROM mf_nav.gold.category_summary
WHERE plan = 'Direct' AND schemes_with_3y >= 5
ORDER BY `Median max drawdown 3Y %`
