import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import  org.apache.log4j.{Level,Logger}

object Comparison {
  def main(args: Array[String]): Unit = {
     System.setProperty("hadoop.home.dir","D:\\Project_softwares\\Hadoop")//("hadoop.home.dir", "D:\\Project_softwares\\Hadoop")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sp = SparkSession.builder.master("local").appName("Crate DataFrame").getOrCreate()
    import sp.sqlContext.implicits._
    val df = sp.read.format("com.databricks.spark.xml").option("header", "true").option("rowTag", "TABLE").option("rootTag", "COL")
      .load("D:\\ProjDocs\\Spark_project_code\\PROD DATA\\S_PROD_INT_1.xml")
    df.printSchema()
    val ProdData1 = df.select(explode($"COL").as("COL"))
      .select($"COL._name",$"COL._VALUE")
    println("ProdData Count",ProdData1.count())
   ProdData1.write.format("com.databricks.spark.csv").save("D:\\ProjDocs\\Spark_project_code\\XMLData\\ProdData1.csv")
    ProdData1.printSchema()
    ProdData1.show()
    val df1 = sp.read.format("com.databricks.spark.xml").option("header", "true").option("rowTag", "TABLE").option("rootTag", "COL")
      .load("D:\\ProjDocs\\Spark_project_code\\QA DATA\\QA DATA\\2158268866031333.xml")
        df1.printSchema()
    val QAData1 = df1.select(explode($"COL").as("COL1"))
      .select($"COL1._name",$"COL1._VALUE")
    println("QAData_Insert Count",QAData1.count())
    QAData1.printSchema()
    QAData1.show()
    QAData1.write.format("com.databricks.spark.csv").save("D:\\ProjDocs\\Spark_project_code\\XMLData\\QAData1.csv")
    val FinData=(QAData1).except(ProdData1)
   FinData.coalesce(1).write.format("com.databricks.spark.csv").save("D:\\ProjDocs\\Spark_project_code\\XMLData\\PQData1.csv")
FinData.show()
 }
}
