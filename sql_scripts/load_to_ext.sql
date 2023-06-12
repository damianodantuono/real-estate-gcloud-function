TRUNCATE TABLE dwhre.EXT_HOUSES;
LOAD DATA INTO dwhre.EXT_HOUSES
FROM FILES (
  FORMAT = 'PARQUET',
  uris = ['gs://scraped-data-gh/raw_data/*.parquet.gzip'
  ]
)
;