package utils

import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2017/7/26.
  */
object SessionMaker {
  def make(locate:String) : SparkSession ={
    val spark = if (locate.equals("local")) SparkSession.builder().master("local[2]").config("spark.sql.warehouse.dir", "D:\\Data\\spark-warehouse").getOrCreate()
    else SparkSession.builder().getOrCreate()
    spark
  }
}
