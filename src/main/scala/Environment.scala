import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.sql.toMongoDataFrame
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Environment {
  val logger = Logger.getLogger(Environment.getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Environment Mohamed & Aziz")
    logger.setLevel(Level.INFO)
    try {
      logger.info("Begin ...")
      Logger.getLogger("org").setLevel(Level.OFF)
      val location = "mongodb://172.31.249.31:27017"
      val database = "environmentdb"
      val collection1 = "polluant"
      val collection2 = "capteur"
      val collection3 = "site"
      val collection4 = "environment"

      val mongoUri1 = s"${location}/${database}.${collection1}"
      val mongoUri2 = s"${location}/${database}.${collection2}"
      val mongoUri3 = s"${location}/${database}.${collection3}"
      val mongoUri4 = s"${location}/${database}.${collection4}"

      implicit val spark: SparkSession = SparkSession.builder()
        .appName("MongoSparkConnector")
        .config("spark.mongodb.input.uri", mongoUri1)
        .config("spark.mongodb.output.uri", mongoUri1)
        .config("spark.mongodb.input.uri", mongoUri2)
        .config("spark.mongodb.output.uri", mongoUri2)
        .config("spark.mongodb.input.uri", mongoUri3)
        .config("spark.mongodb.output.uri", mongoUri3)
        .config("spark.mongodb.input.uri", mongoUri4)
        .config("spark.mongodb.output.uri", mongoUri4)
        .getOrCreate();

      logger.info("Prepare dataset Site")
      val dfSite1 = spark.read.option("header", true)
        .option("header",true)
        .option("inferSchema", true)
        .option("delimiter", ";")
        .csv("hdfs://172.31.249.84:9000/user/hadoopuser/environment_health/raw-environment/dataset1/FR_E2_2021-01-01.csv")

      import org.apache.spark.sql.functions.{coalesce, col, concat, concat_ws, lit, rand, regexp_replace, round, split, when}
      val dfSite1filter =
        dfSite1.filter(col("Polluant") === "NO2" ||
          col("Polluant") === "SO2" ||
          col("Polluant") === "PM2.5" ||
          col("Polluant") === "CO")
      val dfSite1clean =
        dfSite1filter.drop("Polluant","Date de début","Date de fin","valeur brute","Organisme","code zas",
          "Zas","code site", "unité de mesure", "type d'implantation",
          "type d'influence", "discriminant", "Réglementaire", "type d'évaluation",
          "procédure de mesure", "type de valeur", "valeur", "taux de saisie", "couverture temporelle",
          "couverture de données", "code qualité", "validité")
      val dfSite1withoutdoublon = dfSite1clean.distinct()
      val w = Window.orderBy("nom site")
      val dfSite1withindex = dfSite1withoutdoublon.withColumn("IdSite", row_number().over(w))
      val dfSiteFinale = dfSite1withindex
        .select("IdSite", "nom site")

      logger.info("Save refined Site into HDFS")
      dfSiteFinale.coalesce(1).write.mode("overwrite")
      .option("header","true").csv("hdfs://172.31.249.84:9000/user/hadoopuser/environment_health/refined-environment/datasetSite")

      logger.info("Save refined data into mongodb")
      dfSiteFinale.saveToMongoDB(WriteConfig(Map("uri" -> mongoUri3)))

      logger.info("Prepare dataset Polluant")
      val columnsPolluant = Seq("IdPolluant","Libelle")
      val dataPolluant = Seq(("1", "NO2"), ("2", "PM2.5"), ("3", "CO"), ("4", "SO2"))
      val dfPolluantFinale = spark.createDataFrame(dataPolluant).toDF(columnsPolluant:_*)

      logger.info("Save refined Polluant into HDFS")
      dfPolluantFinale.coalesce(1).write.mode("overwrite")
        .option("header","true").csv("hdfs://172.31.249.84:9000/user/hadoopuser/environment_health/refined-environment/datasetPolluant")

      logger.info("Save refined data into mongodb")
      dfPolluantFinale.saveToMongoDB(WriteConfig(Map("uri" -> mongoUri1)))

      logger.info("Prepare dataset Capteur")
      val dfCapteur2 = spark.read.option("header", true)
        .option("header",true)
        .option("inferSchema", true)
        .option("delimiter", ",")
        .csv("hdfs://172.31.249.84:9000/user/hadoopuser/environment_health/raw-environment/dataset2/2017-07_bme280sof.csv")

      val dsCapteur2T1 = dfCapteur2.select("sensor_id","lat","lon")
      val dsCapteur2T2 = dsCapteur2T1.withColumn("IdCapteur",col("sensor_id"))
      val dsCapteur2T6withindex = dsCapteur2T2.withColumn("rowId", monotonically_increasing_id())
      val dsCapteur2T6concat = dsCapteur2T6withindex.withColumn("Marque", functions.concat(lit("Marque "),col("rowId") ))
      val dsCapteur2T3 = dsCapteur2T6concat.withColumn("Latitude",col("lat"))
      val dsCapteur2T4 = dsCapteur2T3.withColumn("Longitude",col("lon"))
      val dsCapteur2Finale1 = dsCapteur2T4.drop("sensor_id","lat","lon")
      val dsCapteur2Finale = dsCapteur2T4.drop("sensor_id","lat","lon","rowId")

      logger.info("Save refined Capteur into HDFS")
      dsCapteur2Finale.coalesce(1).write.mode("overwrite")
        .option("header","true").csv("hdfs://172.31.249.84:9000/user/hadoopuser/environment_health/refined-environment/datasetCapteur")

      logger.info("Save refined data into mongodb")
      dsCapteur2Finale.saveToMongoDB(WriteConfig(Map("uri" -> mongoUri2)))

      logger.info("Join Capteur with Site")
      val datasetcapteurwithsite = dsCapteur2Finale1.join(dfSiteFinale, dsCapteur2Finale1("rowId") === dfSiteFinale("IdSite"), "inner")
      val datasetfinalcapteurwithsite = datasetcapteurwithsite.drop("rowId","Latitude","nom site", "Marque","Longitude")

      logger.info("Prepare dataset Environment")
      // dataset1
      val dfEnvironment = spark.read.option("header", true)
        .option("header",true)
        .option("inferSchema", true)
        .option("delimiter", ";")
        .csv("hdfs://172.31.249.84:9000/user/hadoopuser/environment_health/raw-environment/dataset1/FR_E2_2021-01-01.csv")
        //.csv("C:\\Users\\aziz\\IdeaProjects\\EnvironmentHealth\\FR_E2_2021-01-01.csv")
      import org.apache.spark.sql.functions.{coalesce, col, concat, concat_ws, lit, rand, regexp_replace, round, split, when}
      val dsEnvironmentFilter =
        dfEnvironment.filter(col("Polluant") === "NO2" ||
          col("Polluant") === "SO2" ||
          col("Polluant") === "PM2.5" ||
          col("Polluant") === "CO")
      val dfEnvironmentClean =
        dsEnvironmentFilter.drop("Organisme","code zas",
          "Zas","code site", "unité de mesure", "type d'implantation",
          "type d'influence", "discriminant", "Réglementaire", "type d'évaluation",
          "procédure de mesure", "type de valeur", "valeur", "taux de saisie", "couverture temporelle",
          "couverture de données", "code qualité", "validité")

      logger.info("Join Environment with Polluant")
      val datasetwithpolluant= dfEnvironmentClean.join(dfPolluantFinale, dfEnvironmentClean("Polluant") === dfPolluantFinale("Libelle"), "inner")
      val datasetfinalwithpolluant =   datasetwithpolluant.drop("Polluant","Libelle")

      logger.info("Join Environment with Site")
      val datasetwithsite = datasetfinalwithpolluant.join(dfSiteFinale, datasetfinalwithpolluant("nom site") === dfSiteFinale("nom site"), "inner")
      val datasetfinalwithsite = datasetwithsite.drop("nom site")


      logger.info("Join Environment with Capteur")
      val datasetcapteurwithenvironment = datasetfinalwithsite.join(datasetfinalcapteurwithsite, "IdSite")
        .select("IdCapteur","IdSite", "IdPolluant", "Date de début", "Date de fin", "valeur brute")

      logger.info("Save refined Environment into HDFS")
      datasetcapteurwithenvironment.coalesce(1).write.mode("overwrite")
        .option("header","true").csv("hdfs://172.31.249.84:9000/user/hadoopuser/environment_health/refined-environment/datasetEnvironment")

      logger.info("Save refined data into mongodb")
      datasetcapteurwithenvironment.saveToMongoDB(WriteConfig(Map("uri" -> mongoUri4)))

       logger.info("End ...")
    }
    catch {
      case e: Exception => {
        logger.error(e.getMessage)
        e.printStackTrace()
      }

    }

  }

}