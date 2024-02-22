
from pyspark.sql.functions import when
from pyspark.sql.functions import lit

df.withColumn("nivel", \
   .when((df.brasil_e_regiao_metropolitana == "Brasil"), .lit("BR")) \
     .otherwise(.lit("RM")) \
  ).show()