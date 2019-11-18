package gustavobuosi.nasaAnalysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{functions => f}

object Functions {

  def criaData (dfInicial: DataFrame) : DataFrame ={
    return dfInicial.withColumn("Data", f.substring(dfInicial("_c3"), 2,11))
  }

  def hostsUnicos (df: DataFrame) : Long ={
    return df.select(df("Hosts")).distinct().count()
  }

  def erros404 (df: DataFrame) : Long ={
    return df.filter(df("HTTPCode").cast(IntegerType) === 404).count()
  }

  def distinctUrlErrors (df: DataFrame):DataFrame ={
    val dfDistinctUrls = df.filter(df("HTTPCode").cast(IntegerType) === 404).groupBy(df("URLs")).count()
    return dfDistinctUrls.orderBy(dfDistinctUrls("count").desc).limit(5)
  }

  def erros404Dia (df: DataFrame) : DataFrame ={
    return df.filter(df("HTTPCode").cast(IntegerType) === 404).groupBy(df("Data")).count()
  }

  def bytesTotal (df: DataFrame) : DataFrame = {
    val bytesInteger = df.withColumn("BytesLong", df("Bytes").cast(LongType))
    return bytesInteger.agg(f.sum(bytesInteger("BytesLong")))
  }
}
