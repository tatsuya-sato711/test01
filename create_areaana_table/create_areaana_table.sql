CREATE SCHEMA IF NOT EXISTS [schema];

COMMIT;

DROP TABLE IF EXISTS [schema].t_areaana_[yyyymm]_tmp;

CREATE TABLE [schema].t_areaana_[yyyymm]_tmp
(
	contract_id				VARCHAR(12)   ENCODE LZO
	, mesh3rd_cd			BIGINT        ENCODE LZO
	, mesh4th_cd			BIGINT        ENCODE LZO
	, place					BIGINT        ENCODE LZO -- 自宅:4, 勤務地:3, よく行く場所top3:9,8,7(この順で左からよく行く)
)
DISTKEY (contract_id)
INTERLEAVED SORTKEY (contract_id);

COMMIT;
