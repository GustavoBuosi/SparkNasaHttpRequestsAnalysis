package gustavobuosi.nasaAnalysis

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, TimestampType}
import org.apache.spark.sql.{functions => f}

object Initializer extends App {

val spark = SparkSession.builder()
  .master("local")
  .appName("nasaWordCounter")
  .getOrCreate()

  import spark.implicits._

  val dfInicial = spark.read
      .format("csv")
    .option("quote", "\"")
    .option("multiLine", true)
    .option("inferSchema", "true")
    .option("sep", " ")
    .load("/home/gustavo/Projetos/SparkHiveConnection/NASA_access_log_Jul95", "/home/gustavo/Projetos/SparkHiveConnection/NASA_access_log_Aug95")
      .withColumnRenamed("_c0", "Hosts")
      .withColumnRenamed("_c5", "URLs")
      .withColumnRenamed("_c6","HTTPCode")
      .withColumnRenamed("_c7", "Bytes")


  val df = dfInicial.withColumn("Data", f.substring(dfInicial("_c3"), 2,11))

  //1) Hosts únicos:

  val hostsUnicos = df.select(df("Hosts")).distinct().count()

  //2)Total de erros 404:

  val erros404 = df.filter(df("HTTPCode").cast(IntegerType) === 404).count()

  //3) URLs com mais erros 404
  val dfDistinctUsers = df.filter(df("HTTPCode").cast(IntegerType) === 404).groupBy(df("URLs")).count()
val orderDistinctUsers = dfDistinctUsers.orderBy(dfDistinctUsers("count").desc).limit(5)

  //4) Erros 404 por dia
  val dfData = df.filter(df("HTTPCode").cast(IntegerType) === 404).groupBy(df("Data")).count()
//5) Bytes Totais
  val bytesInteger = df.withColumn("BytesLong", df("Bytes").cast(LongType))
val totalBytes = bytesInteger.agg(f.sum(bytesInteger("BytesLong")))

//  println(hostsUnicos.toString)
//  println(erros404.toString)
  orderDistinctUsers.show()
//  dfData.show()
//  totalBytes.show()
}
