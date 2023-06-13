TRUNCATE TABLE dwhre.EXT_HOUSES;
LOAD DATA INTO dwhre.EXT_HOUSES
FROM FILES (
  FORMAT = 'PARQUET',
  uris = ['gs://{bucket}/{name}']
)
;