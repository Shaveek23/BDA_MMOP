USE mmop_tesla_project;

CREATE EXTERNAL TABLE IF NOT EXISTS post (id bigint, text String,
created_at STRING, source VARCHAR(15))
PARTITIONED BY(ingestion_date string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS PARQUET
LOCATION '/user/mmop/posts';


SELECT id, text, created_at, source FROM post;