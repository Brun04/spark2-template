package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec2(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val popCom = spark.read
      .option("multiline", false)
      .option("mode", "PERMISSIVE")
      .json("data/input/demographie_par_commune.json")
    //popCom.show

    // 1 - Total population in France
    popCom.select(sum($"Population")).show

    // 2 - Population by departement with only INSEE code
    val popDpt = popCom.groupBy($"Departement")
      .sum("Population")
      .orderBy($"sum(Population)".desc)
      .withColumnRenamed("sum(Population)", "Population")
    popDpt.show

    // 3 - Population by departement with INSEE code and departement name
    val dptCsv = spark.read.csv("/home/formation/Documents/Spark/data/departements.txt")
      .toDF("Nom", "Departement")
    popDpt.join(dptCsv, "Departement").show
  }

  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    // Number of tours for each difficulty level
    toursDF.groupBy("tourDifficulty").count().show

    // Minimum, maximum and average of tour price for each difficulty level
    toursDF.groupBy($"tourDifficulty")
      .agg(min($"tourPrice").as("Minimum"), max($"tourPrice").as("Maximum"), avg($"tourPrice").as("Average"))
      .show


  }
}
