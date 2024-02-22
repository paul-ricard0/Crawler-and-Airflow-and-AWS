




df_normalizado = pd.DataFrame()


deltaTable = DeltaTable.forPath(spark, CONSUMER_ZONE)

deltaTable = deltaTable.alias("lake").merge(
    df_normalizado.alias("carga"),
    "lake.id_coleta = carga.id_coleta") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()
  