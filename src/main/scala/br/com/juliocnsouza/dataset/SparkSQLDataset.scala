package br.com.juliocnsouza.dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, count, size, split, sum, udf}

object SparkSQLDataset {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use SparkSession interface
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val schemaPeople = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]



    val  getClassification = (age:Int) => {
      var ageClassification = "baby"
      if (age >= 3 && age <= 12) {
        ageClassification = "child"
      }
      if (age >= 13 && age <= 19) {
        ageClassification = "teenager"
      }
      if (age >= 20 && age <= 65) {
        ageClassification = "adult"
      }
      if (age > 65) {
        ageClassification = "elderly"
      }
      ageClassification
    }

    val getClassificationUDF = udf(getClassification)

    val df = schemaPeople
      .toDF()
      .withColumn("classification", getClassificationUDF(col("age")))

    df.groupBy("classification").agg(count("id")).collect().foreach(println)

    spark.stop()
  }
}
