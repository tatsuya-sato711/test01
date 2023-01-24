DROP TABLE IF EXISTS [schema].numbers;
CREATE TABLE [schema].numbers AS
SELECT 1 AS n
UNION ALL SELECT 2 AS n
UNION ALL SELECT 3 AS n
UNION ALL SELECT 4 AS n
UNION ALL SELECT 5 AS n
UNION ALL SELECT 6 AS n
UNION ALL SELECT 7 AS n
UNION ALL SELECT 8 AS n
UNION ALL SELECT 9 AS n
UNION ALL SELECT 10 AS n
UNION ALL SELECT 11 AS n
UNION ALL SELECT 12 AS n
UNION ALL SELECT 13 AS n
UNION ALL SELECT 14 AS n
UNION ALL SELECT 15 AS n
UNION ALL SELECT 16 AS n
UNION ALL SELECT 17 AS n
UNION ALL SELECT 18 AS n
UNION ALL SELECT 19 AS n
UNION ALL SELECT 20 AS n
UNION ALL SELECT 21 AS n
UNION ALL SELECT 22 AS n
UNION ALL SELECT 23 AS n
UNION ALL SELECT 24 AS n
UNION ALL SELECT 25 AS n
UNION ALL SELECT 26 AS n
UNION ALL SELECT 27 AS n
UNION ALL SELECT 28 AS n
UNION ALL SELECT 29 AS n
UNION ALL SELECT 30 AS n
UNION ALL SELECT 31 AS n
;

DROP TABLE IF EXISTS [schema].calender;
CREATE TABLE [schema].calender
(
   day  TIMESTAMP
);

COMMIT;

INSERT INTO [schema].calender
WITH master AS(
  SELECT date_trunc('month', to_date('[yyyymm]', 'YYYYMM')) + (interval '1 day' * (n-1) ) AS day, date_trunc('month', to_date('[yyyymm]', 'YYYYMM')) AS begin_day
  FROM [schema].numbers
)
SELECT cast(day AS TIMESTAMP)
FROM master
WHERE date_trunc('month', day) = begin_day
;


DROP TABLE IF EXISTS [schema].mesh4th_cd_36_round;
CREATE TABLE [schema].mesh4th_cd_36_round
(
   mesh4th_cd        BIGINT,
   mesh4th_cd_round  BIGINT
)
DISTSTYLE ALL
INTERLEAVED SORTKEY (mesh4th_cd)
;

COMMIT;

INSERT INTO [schema].mesh4th_cd_36_round
WITH master AS (
  SELECT DISTINCT mesh4th_cd, mesh4th_cd/10 AS mesh3rd_cd
  FROM geowork.mesh4th_round
), add_mesh3rd_cd_round AS(
SELECT a.mesh4th_cd, a.mesh3rd_cd, b.mesh3rd_cd_round
FROM master a
INNER JOIN geowork.mesh3rd_round b
USING(mesh3rd_cd)
), mesh3rd_cd_round_to_mesh4th AS(
  SELECT *, mesh3rd_cd_round * 10 + 1 AS mesh4th_cd_round_level_mesh3rd FROM add_mesh3rd_cd_round
  UNION ALL SELECT *, mesh3rd_cd_round * 10 + 2 AS mesh4th_cd_round_level_mesh3rd FROM add_mesh3rd_cd_round
  UNION ALL SELECT *, mesh3rd_cd_round * 10 + 3 AS mesh4th_cd_round_level_mesh3rd FROM add_mesh3rd_cd_round
  UNION ALL SELECT *, mesh3rd_cd_round * 10 + 4 AS mesh4th_cd_round_level_mesh3rd FROM add_mesh3rd_cd_round
)
SELECT mesh4th_cd, mesh4th_cd_round_level_mesh3rd AS mesh4th_cd_round
FROM mesh3rd_cd_round_to_mesh4th
;
COMMIT;

VACUUM REINDEX [schema].mesh4th_cd_36_round;

ANALYZE [schema].mesh4th_cd_36_round;

COMMIT;

DROP TABLE IF EXISTS [schema].t_geohome_[yyyymm];
CREATE TABLE [schema].t_geohome_[yyyymm]
(
    contract_id				VARCHAR(12)   ENCODE LZO
	, mesh4th_cd	BIGINT        ENCODE LZO
	, sum_duration_point			REAL
)
INTERLEAVED SORTKEY (contract_id);

COMMIT;

INSERT INTO [schema].t_geohome_[yyyymm]
WITH calc_sum_duration_point AS(
  SELECT 
    contract_id, 
    estimate_mesh4th_cd AS mesh4th_cd, 
    cast(sum(
             CASE WHEN date_trunc('day',start_log_date) <> date_trunc('day',end_log_date) AND dateadd('min',30,dateadd('hour',3,dateadd('day',1,date_trunc('day',start_log_date)))) <= end_log_date THEN duration_hour * 2 
             WHEN date_trunc('day',start_log_date) = date_trunc('day',end_log_date) AND  start_log_date <= dateadd('min',30,dateadd('hour',3,date_trunc('day',start_log_date))) AND
                  dateadd('min',30,dateadd('hour',3,date_trunc('day',start_log_date))) <= end_log_date THEN duration_hour * 2
             ELSE duration_hour END
            ) AS int) AS cls_sum_duration_point
  FROM [schema].t_geostay_[yyyymm]
  GROUP BY contract_id, estimate_mesh4th_cd
), ranking_by_sum_duration_point AS(
  SELECT contract_id,mesh4th_cd, cls_sum_duration_point, row_number() OVER(PARTITION BY contract_id ORDER BY cls_sum_duration_point DESC) AS rank_sum_duration_point
  FROM calc_sum_duration_point
), limit_by_rank_1 AS (
  SELECT contract_id, mesh4th_cd, cls_sum_duration_point, rank_sum_duration_point AS rank
  FROM ranking_by_sum_duration_point
  WHERE rank_sum_duration_point = 1
)
SELECT contract_id, mesh4th_cd, cls_sum_duration_point AS sum_duration_point
FROM limit_by_rank_1
WHERE rank = 1
;

COMMIT;

VACUUM [schema].t_geohome_[yyyymm];

ANALYZE [schema].t_geohome_[yyyymm];

COMMIT;

DROP TABLE IF EXISTS [schema].t_geowork_[yyyymm];
CREATE TABLE [schema].t_geowork_[yyyymm](
contract_id   VARCHAR     ENCODE LZO
, mesh4th_cd  BIGINT	  ENCODE LZO
, cnt_day     BIGINT	  ENCODE LZO
, sum_duratiON  FLOAT
, ratio_duratiON  FLOAT
)
SORTKEY (contract_id)
;

INSERT INTO [schema].t_geowork_[yyyymm]
WITH param_ratio_limit AS(
  SELECT 0.1 AS ratio_limit
), param_cnt_limit AS(
  SELECT 8 AS cnt_limit
), calc_sum_duratiON AS(
  SELECT contract_id, sum(duration_hour) AS sum_duration
  FROM [schema].t_geostay_[yyyymm]
  GROUP BY contract_id
), calc_home_round AS(
  SELECT a.contract_id, a.mesh4th_cd, b.mesh4th_cd_round
  FROM [schema].t_geohome_[yyyymm] a
  INNER JOIN [schema].mesh4th_cd_36_round b
  ON a.mesh4th_cd = b.mesh4th_cd
), WITHout_home_round AS(
  SELECT 
    a.*
    , c.sum_duration
    , CAST(SUM(a.duration_hour) OVER(PARTITION BY a.contract_id, a.estimate_mesh4th_cd) AS FLOAT) / c.sum_duratiON AS ratio_duration
  FROM [schema].t_geostay_[yyyymm] a
  LEFT JOIN calc_home_round b
  ON a.contract_id = b.contract_id AND a.estimate_mesh4th_cd = b.mesh4th_cd_round
  INNER JOIN calc_sum_duratiON c
  ON a.contract_id = c.contract_id
  WHERE b.contract_id IS NULL
), join_calender AS (
  SELECT a.*, b.day
  FROM WITHout_home_round a
  LEFT JOIN [schema].calender b
  ON b.day BETWEEN date_trunc('day',a.start_log_date) AND a.end_log_date
  CROSS JOIN param_ratio_limit c
  WHERE a.ratio_duratiON >= c.ratio_limit
), calc_cnt_day AS(
  SELECT contract_id, estimate_mesh4th_cd AS mesh4th_cd, sum_duration, ratio_duration, count(DISTINCT day) AS cnt_day
  FROM join_calender
  GROUP BY contract_id, estimate_mesh4th_cd, sum_duration, ratio_duration
), ranking_by_cnt_day AS(
  SELECT *, row_number() OVER(PARTITION BY contract_id ORDER BY cnt_day DESC) AS rank
  FROM calc_cnt_day
  CROSS JOIN param_cnt_limit
  WHERE cnt_day >= cnt_limit
), limit_by_rank AS (
  SELECT contract_id, mesh4th_cd, cnt_day, sum_duration,  ratio_duration
  FROM ranking_by_cnt_day
  WHERE rank = 1
)
SELECT *
FROM limit_by_rank
;


COMMIT;

DROP TABLE IF EXISTS [schema].t_geofreq_[yyyymm];
CREATE TABLE [schema].t_geofreq_[yyyymm](
contract_id   VARCHAR     ENCODE LZO
, mesh4th_cd  BIGINT	  ENCODE LZO
, cnt_day     BIGINT	  ENCODE LZO
, sum_duratiON  FLOAT
, rank     BIGINT	  ENCODE LZO
)
SORTKEY (contract_id)
;

INSERT INTO [schema].t_geofreq_[yyyymm]
WITH calc_home_or_work_area AS(
  SELECT contract_id, mesh4th_cd FROM [schema].t_geohome_[yyyymm]
  UNION ALL SELECT contract_id, mesh4th_cd FROM [schema].t_geowork_[yyyymm]
), calc_home_or_work_round AS(
  SELECT a.contract_id, a.mesh4th_cd, b.mesh4th_cd_round
  FROM calc_home_or_work_area a
  INNER JOIN [schema].mesh4th_cd_36_round b
  ON a.mesh4th_cd = b.mesh4th_cd
), WITHout_home_or_work_round AS(
  SELECT a.*, sum(a.duration_hour) OVER(PARTITION BY a.contract_id, a.estimate_mesh4th_cd) AS sum_duration
  FROM [schema].t_geostay_[yyyymm] a
  LEFT JOIN calc_home_or_work_round b
  ON a.contract_id = b.contract_id AND a.estimate_mesh4th_cd = b.mesh4th_cd_round
  WHERE b.contract_id IS NULL
), join_calender AS (
  SELECT a.*, b.day
  FROM WITHout_home_or_work_round a
  LEFT JOIN [schema].calender b
  ON b.day BETWEEN date_trunc('day',a.start_log_date) AND a.end_log_date
), calc_cnt_day AS(
  SELECT contract_id, estimate_mesh4th_cd AS mesh4th_cd, sum_duration, count(DISTINCT day) AS cnt_day
  FROM join_calender
  GROUP BY contract_id, estimate_mesh4th_cd, sum_duration
  having count(DISTINCT day) >= 2
), ranking_by_cnt_day AS(
  SELECT *, row_number() OVER(PARTITION BY contract_id ORDER BY cnt_day DESC) AS rank
  FROM calc_cnt_day
), limit_by_rank AS (
  SELECT contract_id, mesh4th_cd, cnt_day, sum_duration, rank
  FROM ranking_by_cnt_day
  WHERE rank <= 3
)
SELECT *
FROM limit_by_rank
;

COMMIT;

DROP TABLE IF EXISTS [schema].t_areaana_[yyyymm]_tmp;
CREATE TABLE [schema].t_areaana_[yyyymm]_tmp
(
	contract_id				VARCHAR(12)   ENCODE LZO
	, mesh3rd_cd			BIGINT        ENCODE LZO
	, mesh4th_cd			BIGINT        ENCODE LZO
	, place					BIGINT        ENCODE LZO  -- 自宅:4, 勤務地:3, よく行く場所top3:9,8,7(この順で左からよく行く)
)
DISTKEY (contract_id)
INTERLEAVED SORTKEY (contract_id);

COMMIT;

INSERT INTO [schema].t_areaana_[yyyymm]_tmp 
WITH master AS(
SELECT contract_id, mesh4th_cd/10 AS mesh3rd_cd, mesh4th_cd, 4 AS place FROM [schema].t_geohome_[yyyymm]
UNION ALL SELECT contract_id, mesh4th_cd/10 AS mesh3rd_cd, mesh4th_cd, 3 AS place FROM [schema].t_geowork_[yyyymm]
UNION ALL SELECT contract_id, mesh4th_cd/10 AS mesh3rd_cd, mesh4th_cd, 9 AS place FROM [schema].t_geofreq_[yyyymm] WHERE rank = 1
UNION ALL SELECT contract_id, mesh4th_cd/10 AS mesh3rd_cd, mesh4th_cd, 8 AS place FROM [schema].t_geofreq_[yyyymm] WHERE rank = 2
UNION ALL SELECT contract_id, mesh4th_cd/10 AS mesh3rd_cd, mesh4th_cd, 7 AS place FROM [schema].t_geofreq_[yyyymm] WHERE rank = 3
)
SELECT *
FROM master
;

COMMIT;