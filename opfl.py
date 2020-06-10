
# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Load
## Here, the schema for the routes table is defined. All but the 'stops' columns is defined as a string column.
schema_routes = StructType([
    StructField("airline", StringType(), True),
    StructField("airline_id", StringType(), True),
    StructField("src_airport", StringType(), True),
    StructField("src_airport_id", StringType(), True),
    StructField("dst_airport", StringType(), True),
    StructField("dst_airport_id", StringType(), True),
    StructField("codeshare", StringType(), True),
    StructField("stops", IntegerType(), True),
    StructField("equipment", StringType(), True),
])

## This is where the dataFrame is created, calling upon the format attribute to get Spark to read CSV
routes = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_routes)
    .load("hdfs:///data/openflights/routes.dat")
)

## Call to cache routes to memory and display results
routes.cache()
routes.show()

## Here, the schema for the airports table is defined. Most columns are defined as string columns, save for 'airport_id', which is an interger column
## and the latitude, longitude, altitude and timezone columns, which are all real number columns.

schema_airports = StructType([
  StructField("airport_id", IntegerType(), True),
  StructField("airport", StringType(), True),
  StructField("city", StringType(), True),
  StructField("country", StringType(), True),
  StructField("iata", StringType(), True),
  StructField("icao", StringType(), True),
  StructField("latitude", DoubleType(), True),
  StructField("longitude", DoubleType(), True),
  StructField("altitude", DoubleType(), True),
  StructField("timezone", DoubleType(), True),
  StructField("dst", StringType(), True),
  StructField("tz", StringType(), True),
  StructField("type", StringType(), True),
  StructField("source", StringType(), True),
])

## This is where the airports table is created, again calling the formal attribute to handle csv.
airports = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_airports)
    .load("hdfs:////data/openflights/airports.dat")
)
## Call to cache airports to memory and display results
airports.cache()
airports.show()

# SQL

# Create temp tables

airports.registerTempTable('airports_tbl')
routes.registerTempTable('routes_tbl')

counts_sql = """
    SELECT # outer query
    airports_tbl.airport AS airport_name,
    counts_tbl.size as size
    FROM
    (
        SELECT #inner query
        routes_tbl.src_airport_id AS airport_id, # count source airport destinations
        count(routes_tbl.dst_airport_id) AS size 
        FROM
        routes_tbl 
        GROUP BY airport_id # group by airport_id
        ORDER BY size DESC # order to display biggest at top
    ) counts_tbl
    LEFT JOIN airports_tbl ON counts_tbl.airport_id = airports_tbl.airport_id # join created inner query table to airports_tbl
"""
counts = spark.sql(counts_sql) 
counts.show() # display results

# Pyspark SQL library

from pyspark.sql import functions as F

counts = (
    routes
    # Select only the columns that are needed
    .select(['src_airport_id', 'dst_airport_id'])
    # Group by source and count destinations
    .groupBy('src_airport_id')
    .agg({'dst_airport_id': 'count'})
    .orderBy('count(dst_airport_id)', ascending=False)
    .select(
        F.col('src_airport_id').alias('airport_id'),
        F.col('count(dst_airport_id)').alias('airport_size')
    )
    # Merge with airports to get airport name
    .join(
        airports
        .select(
            F.col('airport_id'),
            F.col('airport').alias('airport_name')
        ),
        on='airport_id',
        how='left'
    )
    # Select columns to be retained
    .select(
        F.col('airport_name'),
        F.col('airport_size')
    )
)
counts.show()


# ***Q1B***


from pyspark.sql import functions as F

counts = (
    routes
    # Group by source and count destinations
    .groupBy('airline')
    .agg({'airline': 'count'})
    .orderBy('count(airline)', ascending=False)
    .select(['airline', F.col('count(airline)').alias('route_count')
]))
counts.show()

# +-------+-----------+
# |airline|route_count|
# +-------+-----------+
# |     FR|       2484|
# |     AA|       2354|
# |     UA|       2180|
# |     DL|       1981|
# |     US|       1960|
# |     CZ|       1454|
# |     MU|       1263|
# |     CA|       1260|
# |     WN|       1146|
# |     U2|       1130|
# |     AF|       1071|
# |     LH|        923|
# |     AZ|        877|
# |     IB|        831|
# |     KL|        830|
# |     ZH|        815|
# |     AB|        798|
# |     FL|        726|
# |     AC|        705|
# |     TK|        658|
# +-------+-----------+
# only showing top 20 rows



# ***Q1C***


counts = (
    routes
    # Group by source and count destinations
    .groupBy('airline')
    .agg(F.countDistinct('dst_airport').alias('distinct_airport_count'))
    .orderBy('distinct_airport_count', ascending=False)
)
counts.show()

# +-------+----------------------+
# |airline|distinct_airport_count|
# +-------+----------------------+
# |     AA|                   432|
# |     UA|                   430|
# |     AF|                   376|
# |     KL|                   359|
# |     DL|                   351|
# |     US|                   337|
# |     AZ|                   271|
# |     TK|                   258|
# |     LH|                   244|
# |     MU|                   222|
# |     BA|                   200|
# |     AC|                   194|
# |     IB|                   194|
# |     CZ|                   193|
# |     CA|                   188|
# |     FR|                   176|
# |     SU|                   154|
# |     AS|                   137|
# |     EK|                   134|
# |     AB|                   133|
# +-------+----------------------+
# only showing top 20 rows


# ***Q1D***

counts = (
    routes
    .where((F.col('src_airport') == 'AKL') | (F.col('dst_airport') == 'AKL'))
)
counts.select(F.col('airline')).distinct().show(30)

# +-------+
# |airline|
# +-------+
# |     AC|
# |     CX|
# |     KE|
# |     NZ|
# |     OZ|
# |     TN|
# |     US|
# |     CA|
# |     ET|
# |     FJ|
# |     LA|
# |     SB|
# |     UA|
# |     CZ|
# |     EK|
# |     JQ|
# |     MH|
# |     SQ|
# |     TK|
# |     VA|
# |     AA|
# |     CI|
# |     HA|
# |     MU|
# |     NF|
# |     NH|
# |     QF|
# |     TG|
# +-------+


# ***Q1E***


tab1 = (
    routes
    # Select only the columns that are needed
    .select([
        F.col('src_airport_id'), 
        F.col('dst_airport_id'),
        F.col('src_airport').alias('iata')])
    .join(
        airports
        .select(
            F.col('iata'),
            F.col('latitude')
        ),
        on='iata',
        how='left'
    ))

tab1 = tab1.select([F.col('src_airport_id'), F.col('dst_airport_id'), F.col('latitude').alias('src_lat')])

tab2 = (
    routes
    # Select only the columns that are needed
    .select([
        F.col('src_airport_id'), 
        F.col('dst_airport_id'),
        F.col('dst_airport').alias('iata')])
    .join(
        airports
        .select(
            F.col('iata'),
            F.col('latitude')
        ),
        on='iata',
        how='left'
    ))

tab2 = tab2.select([F.col('src_airport_id'), F.col('dst_airport_id'), F.col('latitude').alias('dst_lat')])

#new answer for last question

tab1 = tab1.withColumn("id", F.monotonically_increasing_id())
tab1 = tab1.select("id","src_airport_id","dst_airport_id","src_lat")
tab2 = tab2.withColumn("id", F.monotonically_increasing_id())
tab2 = tab2.select("id","src_airport_id","dst_airport_id","dst_lat") 
tab3 = tab1.join(tab2.select(F.col('id'), F.col('dst_lat')), on='id', how='inner')
tab4 = tab3.withColumn("new_col", F.when((tab3["dst_lat"]>0.0) & (tab3["src_lat"]<0.0), 1).otherwise(0))
tab5 = tab4.where("new_col == 1")
tab6 = tab5.groupBy("dst_airport_id").agg({'new_col': 'count'}).orderBy('count(new_col)', ascending=False)


