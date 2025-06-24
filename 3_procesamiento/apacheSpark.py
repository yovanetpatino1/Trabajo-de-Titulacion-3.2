from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, struct, collect_list, countDistinct, avg, size, current_date, months_between, floor
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, LongType, ArrayType
import pyspark
import os
# Crear sesión de Spark con conexión a MongoDB
print("Creando sesión de Spark con MongoDB...")
print(pyspark.__version__)
print("antes de intentar conectassssr ")
#os.environ['JAVA_HOME'] = 'C:/Program Files (x86)/Java/jdk1.8.0_281'
spark = SparkSession.builder \
    .appName("AgrupamientoConNinos") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/control_desnutricion.mediciones") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/control_desnutricion.mediciones_agrupadas") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.local.dir", "C:/SparkTemp") \
    .getOrCreate()
print("descpues de conectar")
# Definir esquemas para las colecciones (buenas prácticas de Big Data)
# Esto mejora el rendimiento y la robustez al cargar los datos
# ======================================================================================================
# *** CAMBIOS PRINCIPALES AQUÍ: ASEGÚRATE DE QUE LOS TIPOS DE DATOS COINCIDAN CON MONGODB ***
# ======================================================================================================

schema_mediciones = StructType([
    StructField("_id", StringType(), True), # Si tu _id es ObjectId, a menudo Spark lo lee como String
    StructField("fecha", DateType(), True),
    StructField("diagnostico_id", StringType(), True),
    StructField("nino_id", StringType(), True), # Generalmente los IDs son Strings
    StructField("centro_id", StringType(), True), # <-- Posible cambio si "0013-" es de aquí
    StructField("edad_meses", IntegerType(), True),
    StructField("imc_z", DoubleType(), True),
    StructField("parroquia_id", StringType(), True), # <-- Posible cambio
    StructField("peso", DoubleType(), True),
    StructField("talla", DoubleType(), True)
])

schema_ninos = StructType([
    StructField("_id", StringType(), True),
    StructField("sexo", StringType(), True),
    StructField("fecha_nacimiento", DateType(), True),
    StructField("idintervalo", StringType(), True), # <-- Posible cambio si "000-1" es de aquí
    StructField("idnivel", StringType(), True) # <-- Posible cambio si "00LEV" es de aquí
])

schema_provincias = StructType([
    StructField("_id", StringType(), True), # <-- Posible cambio
    StructField("nombre", StringType(), True)
])

schema_cantones = StructType([
    StructField("_id", StringType(), True), # <-- Posible cambio
    StructField("nombre", StringType(), True),
    StructField("provincia_id", StringType(), True) # <-- Posible cambio
])

schema_parroquias = StructType([
    StructField("_id", StringType(), True), # <-- Posible cambio
    StructField("nombre", StringType(), True),
    StructField("canton_id", StringType(), True) # <-- Posible cambio
])

schema_intervalos = StructType([
    StructField("_id", StringType(), True), # <-- Posible cambio
    StructField("nombre", StringType(), True)
])

schema_niveles = StructType([
    StructField("_id", StringType(), True), # <-- Posible cambio
    StructField("simbolo", StringType(), True),
    StructField("descripcion", StringType(), True)
])


# Cargar colecciones de MongoDB con esquemas definidos
print("Cargando datos desde MongoDB con esquemas definidos...")
df_mediciones = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/control_desnutricion.mediciones").schema(schema_mediciones).load()
# Las tablas de referencia se marcan para broadcast si son pequeñas, para optimizar los joins.
df_provincias = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/control_desnutricion.provincias").schema(schema_provincias).load().alias("prov")
df_cantones = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/control_desnutricion.cantones").schema(schema_cantones).load().alias("can")
df_parroquias = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/control_desnutricion.parroquias").schema(schema_parroquias).load().alias("parro")
df_centros_salud = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/control_desnutricion.centros_salud").load().alias("centro") # Si 'tipo' se refiere a algo aquí
df_ninos = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/control_desnutricion.ninos").schema(schema_ninos).load().alias("nino")
df_intervalos = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/control_desnutricion.intervalos").schema(schema_intervalos).load().alias("inter")
df_niveles = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/control_desnutricion.niveles").schema(schema_niveles).load().alias("nivel")


# Realizando los joins entre las colecciones y seleccionando columnas de interés
print("Realizando los joins y seleccionando columnas...")
df_processed = df_mediciones.alias("med") \
    .join(df_parroquias, col("med.parroquia_id") == col("parro._id"), "left") \
    .join(df_cantones, col("parro.canton_id") == col("can._id"), "left") \
    .join(df_provincias, col("can.provincia_id") == col("prov._id"), "left") \
    .join(df_ninos, col("med.nino_id") == col("nino._id"), "left") \
    .join(df_intervalos, col("nino.idintervalo") == col("inter._id"), "left") \
    .join(df_niveles, col("nino.idnivel") == col("nivel._id"), "left") \
    .select(
        col("med.nino_id"),
        col("med.fecha"),
        col("med.peso"),
        col("med.talla"),
        col("med.diagnostico_id"),
        col("med.edad_meses"), # Se sigue usando la edad de la medición
        col("med.imc_z"),
        col("med.centro_id"),
        col("prov._id").alias("provincia_id"),
        col("prov.nombre").alias("nombre_provincia"),
        col("can._id").alias("canton_id"),
        col("can.nombre").alias("nombre_canton"),
        col("parro._id").alias("parroquia_id"),
        col("parro.nombre").alias("nombre_parroquia"),
        col("nino.sexo").alias("sexo"),
        col("nino.fecha_nacimiento").alias("fecha_nacimiento_nino"),
        col("inter._id").alias("idintervalo"),
        col("inter.nombre").alias("nombre_intervalo_edad"),
        col("nivel._id").alias("idnivel"),
        col("nivel.simbolo").alias("simbolo_nivel_desnutricion"),
        col("nivel.descripcion").alias("descripcion_nivel_desnutricion")
    )

print("Esquema del DataFrame unido y seleccionado:")
df_processed.printSchema()


# Recalcular o validar edad_meses si se tiene fecha_nacimiento
# Esta es una buena práctica para asegurar la consistencia o para casos donde 'edad_meses'
# no venga directamente en la medición.
df_processed = df_processed.withColumn(
    "edad_meses_calculada",
    floor(months_between(col("fecha"), col("fecha_nacimiento_nino")))
)

# Crear intervalos de edad
print("Creando intervalos de edad...")
df_processed = df_processed.withColumn("INTERVALO_EDAD_GRUPO",
    when(col("edad_meses") <= 12, "0-12 meses")
    .when((col("edad_meses") > 12) & (col("edad_meses") <= 24), "13-24 meses")
    .when((col("edad_meses") > 24) & (col("edad_meses") <= 36), "25-36 meses")
    .when((col("edad_meses") > 36) & (col("edad_meses") <= 60), "37-60 meses")
    .otherwise("Mayor a 60 meses"))

# Crear struct con la información de cada niño (detalle de la medición)
print("Creando struct con la información de cada niño (medición específica)...")
df_processed = df_processed.withColumn("MEDICION_NINO", struct(
    col("nino_id"),
    col("fecha"),
    col("peso"),
    col("talla"),
    col("diagnostico_id"),
    col("edad_meses"),
    col("imc_z")
))

# Agrupar por criterios e incluir lista de mediciones de niños
print("Agrupando los datos por criterios específicos...")
agrupado = df_processed.groupBy(
    "idnivel",
    "simbolo_nivel_desnutricion",
    "descripcion_nivel_desnutricion",
    "provincia_id",
    "nombre_provincia",
    "canton_id",
    "nombre_canton",
    "parroquia_id",
    "nombre_parroquia",
    "centro_id", # Se asume que 'tipo' se refiere a este ID del centro de salud
    "sexo",
    "idintervalo",
    "nombre_intervalo_edad",
    "INTERVALO_EDAD_GRUPO"
).agg(
    countDistinct("nino_id").alias("total_ninos_unicos"), # Contar niños únicos por grupo
    collect_list("MEDICION_NINO").alias("MEDICIONES_DETALLE") # Lista de mediciones detalladas por grupo
)

# Guardar el resultado en MongoDB
print("Guardando los resultados agregados en MongoDB...")
agrupado.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()

# Mostrar ejemplo de los primeros 3 resultados
print("\n--- Muestra de Datos Agrupados (primeras 3 filas) ---")
agrupado.show(3, truncate=False)

# Mostrar el esquema del DataFrame final agregado
print("\n--- Esquema del DataFrame Agrupado ---")
agrupado.printSchema()

# --- Métricas de Procesamiento Adicionales (como se solicitó) ---

print("\n--- Conteo Total de Agrupaciones ---")
print(f"Total de agrupaciones únicas: {agrupado.count()}")

print("\n--- Tamaño Promedio de la Lista de Mediciones por Grupo ---")
agrupado.withColumn("num_mediciones_en_grupo", size(col("MEDICIONES_DETALLE"))) \
    .select(avg("num_mediciones_en_grupo").alias("promedio_mediciones_por_grupo")) \
    .show(truncate=False)

print("\n---  Niveles de Desnutrición Más Representados en Agrupaciones ---")
agrupado.groupBy("idnivel", "descripcion_nivel_desnutricion") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(5, truncate=False)

print("\n--- Distribución de Sexo en Agrupaciones ---")
agrupado.groupBy("sexo") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(truncate=False)

# Detener sesión de Spark
spark.stop()
print("Sesión de Spark detenida.")