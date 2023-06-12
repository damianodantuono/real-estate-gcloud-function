MERGE INTO `real-estate-analyser-dv.dwhre.DIM_CITY` as target
using (
  SELECT ROW_NUMBER() OVER() + (SELECT COALESCE(MAX(CITY_ID), -1) FROM dwhre.DIM_CITY) as CITY_ID, CITY_NAME FROM (SELECT DISTINCT CITY_NAME FROM dwhre.EXT_HOUSES)
) as source
on source.CITY_NAME = target.CITY_NAME
WHEN NOT MATCHED BY target THEN
INSERT (
  CITY_ID,
  CITY_NAME
)
VALUES (
  source.CITY_ID,
  source.CITY_NAME
)
;