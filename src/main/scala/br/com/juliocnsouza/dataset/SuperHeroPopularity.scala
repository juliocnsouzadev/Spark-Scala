package br.com.juliocnsouza.dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, max, size, split, sum, min}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/** Find the superhero co-appearances. */
object SuperHeroPopularity {

  case class SuperHeroNames(id: Int, name: String)

  case class SuperHero(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("SuperHeroPopularity")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading Marvel-names.txt
    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    // Build up a hero ID -> name Dataset
    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines: Dataset[SuperHero] = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val column: Column = col("value")
    val splicedColumn: Column = split(column, " ")
    val splicedColumnSize: Column = size(splicedColumn)

    val connections: DataFrame = lines
      .withColumn("id", splicedColumn(0))
      .withColumn("connections", splicedColumnSize - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val leastPopularValue: Long = connections.agg(min("connections")).first().getLong(0)
    val mostUnpopular: Dataset[Row] = connections.filter(connections("connections") === leastPopularValue)

    val mostPopularValue: Long = connections.agg(max("connections")).first().getLong(0)
    val mostPopular: Dataset[Row] = connections.filter(connections("connections") === mostPopularValue)

    val finalMostUnpopular= mostUnpopular.join(names, usingColumn = "id")
    val finalMostPopular = mostPopular.join(names, usingColumn = "id")

    println("Most Popular Super Heroes")
    finalMostPopular.sort($"name").show()

    println("\nMost Unpopular Super Heroes")
    finalMostUnpopular.sort($"name").show()

  }
}
