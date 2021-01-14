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

    popCom.select(sum($"Population")).show

    val popDpt = popCom.groupBy($"Departement")
      .sum("Population")
      .orderBy($"sum(Population)".desc)
      .withColumnRenamed("sum(Population)", "Population")
    popDpt.show

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

    toursDF.groupBy("tourDifficulty").count().show

    toursDF.select($"tourDifficulty", $"tourPrice").groupBy($"tourDifficulty")
      .count()
      .select(min($"tourPrice"), max($"tourPrice"), avg($"tourPrice"))
      .show

  }
}
