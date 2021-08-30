package com.stratio.module.sparta.custom.transform
import com.stratio.sparta.sdk.lite.xd.batch.LiteCustomXDBatchTransform
import com.stratio.sparta.sdk.lite.batch.models.{OutputBatchTransformData, ResultBatchData}
import org.apache.spark.sql.crossdata.XDSession

import java.math.BigDecimal
import info.debatty.java.stringsimilarity.{Cosine, JaroWinkler, RatcliffObershelp,Damerau}

class CustomDistance(xdSession: XDSession, properties: Map[String, String])
  extends LiteCustomXDBatchTransform(xdSession, properties) {

  override def transform(inputData: Map[String, ResultBatchData]): OutputBatchTransformData = {
    udf_register()
    val batchData = inputData.iterator.next()._2

    OutputBatchTransformData(
      batchData.data,
      batchData.schema
    )

  }

  private def udf_register(): Unit ={
    xdSession.sqlContext.udf.register(
      "UDF_LEVENSHTEIN",
      (str1:String, str2:String) => similarity_ratio(str1,str2))

    xdSession.sqlContext.udf.register(
      "UDF_JAROWINK",
      (str1:String, str2:String) => JaroWinkDistance(str1,str2))

    xdSession.sqlContext.udf.register(
      "UDF_RATCLIFF",
      (str1:String, str2:String) => RatcliffosDistance(str1,str2))


  }

  private def similarity_ratio(s1: String, s2: String): Int = {

    var ss1=s1
    var ss2=s2

    if (ss1.length < ss2.length){
      val swap = s1
      ss1 = ss2
      ss2 = swap
    }

    val bigLen = ss1.length
    if (bigLen == 0) return 1.toInt /* both strings are zero length */
    val a = new BigDecimal(((bigLen - computeEditDistance(ss1, ss2)) / bigLen.toDouble) * 100)
    val ratio_val = a.intValue
    ratio_val
  }

  private def computeEditDistance(s1: String, s2: String): Int = {
    var ss1 : String = s1.toLowerCase
    var ss2 : String = s2.toLowerCase
    val costs = new Array[Int](ss2.length + 1)
    for (i <- 0 to ss1.length) {
      var lastValue = i
      for (j <- 0 to ss2.length) {
        if (i == 0) costs(j) = j
        else if (j > 0) {
          var newValue = costs(j - 1)
          if (ss1.charAt(i - 1) != ss2.charAt(j - 1)) newValue = Math.min(Math.min(newValue, lastValue), costs(j)) + 1
          costs(j - 1) = lastValue
          lastValue = newValue
        }
      }
      if (i > 0) costs(s2.length) = lastValue
    }
    costs(s2.length)
  }

  def JaroWinkDistance(str1: String,str2: String): Int= {
    var jaro= new JaroWinkler
    var sim = (jaro.similarity(str1,str2)*100).toInt
    sim
  }


  def RatcliffosDistance(str1: String,str2: String): Int= {
    var rtos= new RatcliffObershelp
    var sim = (rtos.similarity(str1,str2)*100).toInt
    sim
  }





}