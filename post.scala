package com.mindtree

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import slick.driver.PostgresDriver.api._
import slick.lifted.{ProvenShape, Tag}
//import org.apache.spark.sql.functions._



case class Minds(Mid: String,Mname: String,Mtrack: String,Mproject: String)

class MMinds(tag: Tag) extends Table[Minds](tag, "mm") {

  val Mid: Rep[String] = column[String]("Mid", O.PrimaryKey)
  val Mname: Rep[String] = column[String]("Mname")
  val Mtrack: Rep[String] = column[String]("Mtrack")
  val Mproject: Rep[String] = column[String]("Mproject")

  override def * : ProvenShape[Minds] = (Mid,Mname,Mtrack,Mproject) <> (Minds.tupled, Minds.unapply)
}

object post {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Project_softwares\\Hadoop")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val dbUrl = "jdbc:postgresql://localhost:5433/postgres"
    val dbUsername = "postgres"
    val dbPassword = "Mindtree@12"
    val dbDriver = "org.postgresql.Driver"
    val db = Database.forURL(url = dbUrl, driver = dbDriver, user = dbUsername, password = dbPassword)
    val mm = TableQuery[MMinds]

     /*val queries = mm.schema.create
     val setup = db.run(queries)
     Await.result(setup, 5L.seconds)
*/

    val sqlContext = SparkSession.builder().appName("sparkDB").master("local").getOrCreate()
    val FilePath = "D:\\dbdata.csv"
    val csvFrame: DataFrame = sqlContext.read.format("com.databricks.spark.csv") .option("header", "true") .load(FilePath)
    val jdbcUrl = "jdbc:postgresql://localhost:5433/postgres?user=postgres&password=Mindtree@12&stringtype=unspecified"
    //csvFrame.foreach()
    val connectionProperties = new java.util.Properties()

    csvFrame.write .mode(SaveMode.Overwrite) .jdbc( url = jdbcUrl, table = "mm", connectionProperties = connectionProperties)
  }
}