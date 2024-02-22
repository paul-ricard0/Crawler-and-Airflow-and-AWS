CREATE EXTERNAL TABLE acss_suinos_bolsa (
    estado string, 
    preco string, 
    data_cotacao string, 
    ultima_atualizacao string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://XXXXXX/fat/_symlink_format_manifest'