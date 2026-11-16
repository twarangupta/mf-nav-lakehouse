-- Tile 6: data quality per load date (bar/line chart).
-- Quarantined = rows that broke a hard rule; Known = expected zero NAVs of segregated
-- portfolios (recorded, not counted against the gate); Warnings = big Growth NAV moves kept.
SELECT
  load_date                                          AS `Load date`,
  coalesce(source_kind, 'warning only')              AS `Source`,
  coalesce(data_rows, 0)                             AS `Data rows`,
  coalesce(quarantined_counted_rows, 0)              AS `Quarantined`,
  coalesce(quarantined_known_rows, 0)                AS `Known exclusions`,
  warnings_on_nav_date                               AS `Warnings`
FROM mf_nav.gold.dq_daily
ORDER BY `Load date`
