package gustavobuosi.nasaAnalysis

import org.apache.avro.generic.GenericData.StringType
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, TimestampType}
import org.apache.spark.sql.{functions => f}
import gustavobuosi.nasaAnalysis.Functions

object Application extends App {

  val spark = SparkSession.builder()
  .master("local")
  .appName("nasaWordCounter")
  .getOrCreate()

  LogManager.getRootLogger.setLevel(Level.WARN)

  import spark.implicits._

  //Criação do DataFrame a partir do arquivo, renomeando apenas as colunas que importam para resolver as questões
    val dfInicial = spark.read.format("csv")
    .option("quote", "\"")
    .option("multiLine", true)
    .option("inferSchema", "true")
    .option("sep", " ")
    .load("/home/gustavo/Projetos/Arquivos/NASA_access_log_Jul95", "/home/gustavo/Projetos/Arquivos/NASA_access_log_Aug95")
      .withColumnRenamed("_c0", "Hosts")
      .withColumnRenamed("_c5", "URLs")
      .withColumnRenamed("_c6","HTTPCode")
      .withColumnRenamed("_c7", "Bytes")

  //Criação da coluna Data para a questão 3:

  val df = Functions.criaData(dfInicial)

  //1) Hosts únicos:

  val dfHostsUnicos = Functions.hostsUnicos(df)

  //2)Total de erros 404:

  val dfErros404 = Functions.erros404(df)

  //3) URLs com mais erros 404

  val dfOrderDistinctUrls = Functions.distinctUrlErrors(df)

  //4) Erros 404 por dia

  val df404Diarios = Functions.erros404Dia(df)

//5) Bytes Totais

  val dfTotalBytes = Functions.bytesTotal(df)

  println("Resposta da primeira questão:")
  println(dfHostsUnicos.toString)
  println("Resposta da segunda questão:")
  println(dfErros404.toString)
  println("Resposta da terceira questão:")
  dfOrderDistinctUrls.show()
  println("Resposta da quarta questão:")
  df404Diarios.show()
  println("Resposta da quinta questão:")
  dfTotalBytes.show()
}
