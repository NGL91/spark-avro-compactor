package ie.ianduffy.spark.avro.compactor

import ie.ianduffy.spark.avro.compactor.Utils._
import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object MySchemaConversions {
  def createConverterToSQL(avroSchema: Schema, sparkSchema: DataType): GenericRecord => Row =
    Utils.createConverterToSQL(avroSchema, sparkSchema).asInstanceOf[GenericRecord => Row]
}

object MyObject{
  def myConverter(record: GenericRecord,
                  myAvroRecordConverter: GenericRecord => Row): Row =
    myAvroRecordConverter.apply(record)
}

object Job {

  private val log = LoggerFactory.getLogger(Job.getClass.getName.replace("$", ""))

  def run(spark: SparkSession, schemaRegistry: SchemaRegistryClient, jobConfig: JobConfig): Unit = {
    val schema: Schema = {
      val latestSchemaMetadata: SchemaMetadata = schemaRegistry.getLatestSchemaMetadata(jobConfig.schemaRegistrySubject)
      val id: Int = latestSchemaMetadata.getId
      schemaRegistry.getById(id)
    }

    implicit val sparkConfig: Configuration = spark.sparkContext.hadoopConfiguration
    sparkConfig.set("avro.schema.input.key", schema.toString())
    sparkConfig.set("avro.schema.output.key", schema.toString())

    if (System.getenv("local") != null) {
      sparkConfig.set("fs.s3n.awsAccessKeyId", spark.conf.get("spark.hadoop.fs.s3n.access.key"))
      sparkConfig.set("fs.s3n.awsSecretAccessKey", spark.conf.get("spark.hadoop.fs.s3n.secret.key"))
    }

    val inputPath: Path = new Path(jobConfig.input)
    val outputPath: Path = new Path(jobConfig.output)

    val fs: FileSystem = inputPath.getFileSystem(sparkConfig)

    // avoid raising org.apache.hadoop.mapred.FileAlreadyExistsException
    if (jobConfig.overrideOutput) {
      log.info("Delete output path")
      fs.delete(outputPath, true)
    }

    // from fileSystem prefix with s3 the default is 64MB and can be overwitten by fs.s3.block.size
    // from fileSystem prefix with s3a the default is 32MB and can be overwitten by setting fs.s3a.block.size
    val outputBlocksize: Long = fs.getDefaultBlockSize(outputPath)
    // Where inputPath is of the form s3://some/path

    val check = fs.exists(inputPath)
    if (! check) {
      log.info(s"Can not find input path: $inputPath")
      return None
    }
    val inputPathSize: Long = fs.getContentSummary(inputPath).getSpaceConsumed

    val numPartitions: Int = Math.max(1, Math.floor((inputPathSize / CompressionRatio.AVRO_SNAPPY) / outputBlocksize).toInt)
    log.debug(
      s"""outputBlocksize: $outputBlocksize
         | inputPathSize: $inputPathSize
         | splitSize: $numPartitions
       """.stripMargin)

    val rdd = readHadoopFile(spark, inputPath.toString)

    if (jobConfig.primaryKey != null && jobConfig.timestampKey != null) {
      val filtered_df = compaction(spark, schema, jobConfig.primaryKey, jobConfig.timestampKey, rdd)
      filtered_df.repartition(numPartitions).write.format("avro").save(outputPath.toString)
    } else {
      rdd.coalesce(numPartitions)
        .saveAsNewAPIHadoopFile(
          outputPath.toString,
          classOf[AvroKey[GenericRecord]],
          classOf[NullWritable],
          classOf[AvroKeyOutputFormat[GenericRecord]],
          sparkConfig
        )
    }
    if (jobConfig.flushInput) {
      log.info("Delete input path")
      fs.delete(inputPath, true)
    }

  }

  def compaction(spark: SparkSession, schema: Schema, primaryKey: String, timestampKey: String, rdd: RDD[(AvroKey[GenericRecord], NullWritable)]): DataFrame = {
    val myAvroSchema = SchemaConverters.toSqlType(schema).dataType
    val rddSchema =myAvroSchema.asInstanceOf[StructType]
    val myAvroRecordConverter = MySchemaConversions.createConverterToSQL(schema, myAvroSchema)
    var rowRDD = rdd.map(record =>
      MyObject.myConverter(record._1.datum(), myAvroRecordConverter))
    val df = spark.createDataFrame(rowRDD, rddSchema)

    val windowSpec = Window.partitionBy(df(primaryKey)).orderBy(df(timestampKey).desc)

    val maxColName = "max" ++ primaryKey

    val filtered_df = df.withColumn(maxColName, first(timestampKey).over(windowSpec))
      .select("*").where(col(maxColName) === col(timestampKey))
      .drop(maxColName)

    filtered_df
  }
}
