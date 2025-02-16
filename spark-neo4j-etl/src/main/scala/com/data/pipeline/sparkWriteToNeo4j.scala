package com.data.pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.FileInputStream
import java.util.Properties

object sparkWriteToNeo4j{
  def main(args:Array[String]): Unit = {
    // read config from application.properties file
    val properties = new Properties()
    val inputStream = new FileInputStream("")
    properties.load(inputStream)

    val inputFilePath = properties.getProperty("input.file.path")
    val neo4jHost = properties.getProperty("neo4j.host")
    val neo4jPort = properties.getProperty("neo4j.port")
    val neo4jUsername = properties.getProperty("neo4j.username")
    val neo4jPassword = properties.getProperty("neo4j.password")

    // initialise spark session
    val spark = SparkSession.builder().appName("spark_neo4j").master("local[*]").getOrCreate()

    // read csv file
    val df = spark.read.option("header","true").option("inferSchema","true").csv(inputFilePath)

    // write customer id and gender as nodes and relationship between them
    df.select("CustomerID","Genre").write
      .mode("Overwrite")
      .format("org.neo4j.spark.DataSource")
      .option("relationship","GENDER")
      .option("relationship.save.strategy","keys")
      .option("relationship.source.save.mode","Overwrite")
      .option("relationship.source.labels",":CustomerID")
      .option("relationship.source.node.properties","CustomerID:ID")
      .option("relationship.source.node.keys","CustomerID:ID")
      .option("relationship.target.save.mode","Overwrite")
      .option("relationship.target.labels",":Gender")
      .option("relationship.target.node.properties","Genre:Gender")
      .option("relationship.target.node.keys","Genre:Gender")
      .option("url","bolt://"+neo4jHost+":"+neo4jPort)
      .option("authentication.type","basic")
      .option("authentication.basic.username",neo4jUsername)
      .option("authentication.basic.password",neo4jPassword)
      .save()


  }

}