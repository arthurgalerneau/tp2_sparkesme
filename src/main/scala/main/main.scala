package main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

object main {


  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions._

    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    val js = sparkSession.read.json("config")
    var jsc = js.collect()
    var nbd = jsc(2)(0).toString
    nbd = nbd.replace("\t\"fillWithDaysAgo\": ", "")
    var nb2 = nbd.toInt

    var inter = "INTERVAL " + nb2 + " days"

    val df = sparkSession.read.option("header", false).option("delimiter", ",").option("inferSchema", true).csv("data")
    val df1 = df.withColumnRenamed("_c0", "amount")
    val df2 = df1.withColumnRenamed("_c1", "base_currency")
    val df3 = df2.withColumnRenamed("_c2", "currency")
    val df4 = df3.withColumnRenamed("_c3", "exchange_rate")
    val df5 = df4.withColumn("date", regexp_extract(split(input_file_name(), "/").getItem(9).cast(StringType), "(\\d{8})",1))
    val df51 = df5.withColumn("date", to_date(col("date"), "ddMMyyyy"))
    val df52 = df51.withColumn("date_ref",col("date") - expr(inter))
    val df53 = df52.withColumn("date_ref_desecours",col("date") - expr("INTERVAL 1 days"))
    var df6 = df53.sort(asc("date"))
    df6.show(10000000)

    var dates = df6.select(col("date")).collect()
    var date_u = dates.distinct

    for( date <- date_u)
    {
      var nb = df6.filter(col("date") === date(0)).count()
      if(nb < 8)
      {
        var day_d = df6.filter(col("date") === date(0)).collect()
        var z = 0
        for (date <- date_u) {
          if (date(0).toString == day_d(0)(5).toString){
            z = 1
          }

        }
        var y = 0
        if (z == 0 ){
          y = 6
        }
        else{
          y = 5
        }
        val day_b = df6.filter(col("date") === day_d(0)(y)).collect()
        var nb_add = 8 - nb
        var i = 0
        while( i < nb_add )
        {
            val columns = Seq("amount", "base_currency", "currency", "exchange_rate", "date", "date_ref", "date_ref_desecours")
            var add = sparkSession.createDataFrame(Seq((day_b(i)(0).toString, day_b(i)(1).toString,day_b(i)(2).toString,day_b(i)(3).toString,date(0).toString, day_b(i)(4).toString, day_b(i)(4).toString))).toDF(columns:_*)
            df6 = df6.union(add)
            i = i + 1
        }
      }
    }
      val df7 = df6.drop("date_ref")
      val df8 = df7.drop("date_ref_desecours")
      df8.show(100000)
      df8.write.mode(SaveMode.Overwrite).partitionBy("date").csv("part2")
  }

}
