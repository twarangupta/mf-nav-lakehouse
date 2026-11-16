-- Tile 3: rolling 1-year return at each month end (line chart, one line per fund).
-- Use a multi-select dashboard filter on `Fund` to choose which funds to show.
SELECT
  r.month                                         AS `Month`,
  concat(m.scheme_name, ' (', m.plan, ')')        AS `Fund`,
  m.category_group                                AS `Category`,
  round(r.return_1y * 100, 2)                     AS `Rolling 1Y return %`
FROM mf_nav.gold.rolling_return_1y_monthly r
JOIN mf_nav.gold.fund_metrics m USING (scheme_code)
WHERE m.is_active
