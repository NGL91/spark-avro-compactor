package ie.ianduffy.spark.avro.compactor

import ie.ianduffy.spark.avro.compactor.Utils._
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.slf4j.LoggerFactory
import java.time.LocalDate

import scala.util.{Failure, Success, Try}

object Runner extends App {

  private val log = LoggerFactory.getLogger(Runner.getClass.getName.replace("$", ""))

  log.info(s"args: ${args.toSeq}")
  private var config = JobConfig.parse(args)
  config = customConfig(config)

  private val schemaRegistry = new CachedSchemaRegistryClient(config.schemaRegistryUrl, 10000)

  log.info(s"Running with application config $config")

  System.setProperty("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")

  if (System.getenv("local") != null) {
    log.info(s"Running with embedded spark")
    runLocally(config)
  } else {
    log.info("Running with remote spark")
    run(config)
  }

  /**
    * Update input-output = input-output/
    *                                   year/month/day of previous day in case of isDaylyRun
    *                                   year/month     of previous day in case of isMonthlyRun
    */
  def customConfig(config: JobConfig): JobConfig = {

    import java.time.temporal.ChronoUnit
    val previousDay = LocalDate.now.minus(1, ChronoUnit.DAYS)
    val year = previousDay.getYear

    var month = previousDay.getMonthValue.toString
    if (month.size != 2) {
      month = "0" ++ month
    }

    var day = previousDay.getDayOfMonth.toString
    if (day.size != 2) {
      day = "0" ++ day
    }

    var newInput = config.input
    var newOutput = config.output
    if (config.isDaylyRun) {
      val path = year.toString ++ "/" ++ month ++ "/" ++ day
      newInput = config.input ++ "/" ++ path
      newOutput = config.output ++ "/" ++ path

    } else if (config.isMonthlyRun) {
      val path = year.toString ++ "/" ++ month
      newInput = config.input ++ "/" ++ path
      newOutput = config.output ++ "/" ++ path

    }

    JobConfig(input = newInput,
              output = newOutput,
              schemaRegistrySubject=config.schemaRegistrySubject,
              schemaRegistryUrl = config.schemaRegistryUrl,
              flushInput = config.flushInput,
              overrideOutput=config.overrideOutput,
              primaryKey = config.primaryKey,
              timestampKey = config.timestampKey
    )
  }

  def runLocally(config: JobConfig) = {
    System.setProperty("spark.master", "local[*]")
    System.setProperty("spark.app.name", "compactor")

    // Set aws credential
    System.setProperty("spark.hadoop.fs.s3n.endpoint", "s3-us-west-2.amazonaws.com")
    System.setProperty("spark.hadoop.fs.s3n.access.key", "")
    System.setProperty("spark.hadoop.fs.s3n.secret.key", "")

    val spark = createSparkSession

    log.info(s"Running with spark configuration: ${spark.conf.getAll}")

    Try {
      Job.run(spark, schemaRegistry, config)
    } match {
      case Success(_) =>
        spark.close()
        System.exit(0)
      case Failure(e) =>
        spark.close()
        e.printStackTrace()
        System.exit(1)
    }
  }

  def run(config: JobConfig) = {
    val spark = createSparkSession
    log.info(s"Running with configuration: ${spark.conf.getAll}")
    Job.run(spark, schemaRegistry, config)
  }

}
