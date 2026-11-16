-- Tile 2: top 10 funds by 3-year CAGR within each category group and plan.
-- Use a dashboard filter on `Category` and `Plan` to pick one list.
SELECT
  category_group                          AS `Category`,
  plan                                    AS `Plan`,
  rank_cagr_3y_in_category                AS `Rank`,
  scheme_name                             AS `Fund`,
  fund_house                              AS `Fund house`,
  round(cagr_3y * 100, 2)                 AS `3Y CAGR %`,
  round(cagr_5y * 100, 2)                 AS `5Y CAGR %`,
  round(return_1y * 100, 2)               AS `1Y return %`,
  round(volatility_1y * 100, 2)           AS `Volatility 1Y %`,
  round(max_drawdown_3y * 100, 2)         AS `Max drawdown 3Y %`,
  latest_nav                              AS `Latest NAV`,
  latest_nav_date                         AS `NAV date`
FROM mf_nav.gold.fund_metrics
WHERE is_active
  AND rank_cagr_3y_in_category <= 10
ORDER BY `Category`, `Plan`, `Rank`
