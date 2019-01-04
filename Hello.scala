package Hello

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._



object Hello {
      def main(args: Array[String]): Unit = {

            Logger.getLogger("org").setLevel(Level.ERROR)
            System.setProperty("hadoop.home.dir", "D:\\Software\\Hadoop")
            val SPS = new SparkSession.Builder().appName("Kafka To Kafka").master("local").getOrCreate()

            val df = SPS
                  .readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .option("subscribe", "sender")
                  .load()
            import SPS.implicits._
            val df1 = df.selectExpr("CAST(value AS STRING)").as[(String)]
            //  .select(functions.from_json($"value", mySchema).as("data"), $"timestamp")
            //  .select("data.*", "timestamp")
             df1.writeStream
                  .format("console")
                  .option("truncate","true")
                  .start()
                  .awaitTermination()

      }
}
