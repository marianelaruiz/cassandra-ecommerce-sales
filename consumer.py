from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, expr, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
from pyspark.sql.functions import col, window, to_timestamp

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaEcommerceConsumer") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema of the incoming JSON messages
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_document", StringType(), True),
    StructField("products", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("price", DoubleType(), True)
    ]))),
    StructField("total_value", DoubleType(), True),
    StructField("sale_date", StringType(), True)
])

# Connect to Kafka and read the stream
ventas_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales-events") \
    .option("startingOffsets", "latest") \
    .load()

# Transform Kafka message from binary to JSON
ventas_df = ventas_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")



# 👉 Convertir sale_date (string) a timestamp
ventas_df = ventas_df.withColumn("sale_timestamp", to_timestamp(col("sale_date"), "dd/MM/yyyy HH:mm:ss"))


# Explode products array
products_df = ventas_df.select(
    col("sale_timestamp"),
    explode("products").alias("product")
)

# Procesar columnas y calcular el total
products_df = products_df.select(
    col("sale_timestamp"),
    col("product.name").alias("product_name"),
    col("product.quantity").cast("int").alias("quantity"),
    col("product.price").alias("price")
).withColumn("total_value", expr("quantity * price"))



products_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
    

# Escribir en Cassandra
query = products_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "ecommerce") \
    .option("table", "sales") \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoints") \
    .start()



try:
    print("Iniciando o consumo de dados...")
    query.awaitTermination()
except KeyboardInterrupt:
    print("Interrumpido manualmente. Cerrando stream...")
    query.stop()

