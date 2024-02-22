SELECT *
FROM consumer_zone.iea_diario
ORDER BY date_parse(data, '%d/%m/%Y') DESC 


