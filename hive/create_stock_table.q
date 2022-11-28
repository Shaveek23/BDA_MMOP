USE mmop_tesla_project;

CREATE EXTERNAL TABLE IF NOT EXISTS stock (s STRING,
p FLOAT, t STRING, v FLOAT, c STRING)
PARTITIONED BY(ingestion_date string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS PARQUET
LOCATION '/user/mmop/stock';

ALTER TABLE stock ADD IF NOT EXISTS PARTITION (ingestion_date='2022-11-17') LOCATION '/user/mmop/stock/20221117';
"ALTER TABLE stock ADD IF NOT EXISTS PARTITION (ingestion_date='${now():format('yyyy-MM-dd')}') LOCATION '#{hdfs.stock.dir}/${now():format('yyyyMMdd')}';"