VACUUM REINDEX [schema].t_areaana_[yyyymm]_tmp;

ANALYZE [schema].t_areaana_[yyyymm]_tmp;

DROP TABLE IF EXISTS [schema].t_areaana_[yyyymm];

ALTER TABLE [schema].t_areaana_[yyyymm]_tmp RENAME TO t_areaana_[yyyymm];

COMMIT;
