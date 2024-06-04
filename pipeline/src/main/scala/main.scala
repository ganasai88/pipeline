package org.pipe.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.nio.file.{Files, Paths}
import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("NewProject")
      .master("local[*]")
      .getOrCreate()

    // Read the JSON configuration file
    val jsonFilePath = "C:\\Users\\ganas\\NewScala\\pipeline\\src\\main\\scala\\config.json"
    val jsonString = Source.fromFile(jsonFilePath).mkString

    // Parse JSON
    implicit val formats: DefaultFormats.type = DefaultFormats
    val json = parse(jsonString)

    // Extract file input, transform, and file write details
    val fileInputs = (json \ "file_input").extract[List[FileInput]]
    val transforms = (json \ "transform").extract[List[Transform]]
    val fileWrites = (json \ "file_write").extract[List[FileWrite]]

    // Read CSV files into DataFrames and register them as temporary views with renamed columns
    fileInputs.foreach { fileInput =>
      val df = readCsv(spark, fileInput.path)
      val renamedDf = renameColumns(df, fileInput.table_name)
      renamedDf.createOrReplaceTempView(fileInput.table_name)
    }

    // Execute transformations
    transforms.foreach { transform =>
      val result = spark.sql(transform.sql)

      // Find the corresponding file write details
      val fileWrite = fileWrites.find(_.file_name == transform.table_name).getOrElse {
        throw new RuntimeException(s"No file write configuration found for table ${transform.table_name}")
      }

      // Ensure the output directory exists
      val outputPath = Paths.get(fileWrite.file_output)
      if (!Files.exists(outputPath.getParent)) {
        Files.createDirectories(outputPath.getParent)
      }

      // Write the result DataFrame to a CSV file
      result.write.format("csv")
        .option("header", "true") // Write the column names as headers
        .mode("overwrite") // Use overwrite mode for simplicity
        .save(fileWrite.file_output)
    }

    // Stop SparkSession
    spark.stop()
  }

  // Case classes to represent the JSON structure
  case class FileInput(source_type: String, table_name: String, path: String)
  case class Transform(table_name: String, sql: String)
  case class FileWrite(file_name: String, file_output: String)

  // Function to read CSV file into a DataFrame
  def readCsv(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", value = true)
      .csv(path)
  }

  // Function to rename columns to avoid conflicts during joins
  def renameColumns(df: DataFrame, tableName: String): DataFrame = {
    val renamedColumns = df.columns.map(colName => df.col(colName).as(s"${tableName}_$colName"))
    df.select(renamedColumns: _*)
  }
}
