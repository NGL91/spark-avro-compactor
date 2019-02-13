package ie.ianduffy.spark.avro.compactor

import scopt.OptionParser

case class JobConfig(input: String = null,
                     output: String = null,
                     schemaRegistryUrl: String = null,
                     schemaRegistrySubject: String = null,
                     flushInput: Boolean = false,
                     overrideOutput: Boolean = true,
                     primaryKey: String = null,
                     timestampKey: String = null,
                     isDaylyRun: Boolean = false,
                     isMonthlyRun: Boolean = false)

object JobConfig {

  val default = JobConfig()

  val parser = new OptionParser[JobConfig]("avro-concat") {
    opt[String]("input")
      .valueName("<input-path>")
      .text("the s3-path to the input")
      .required()
      .action((v, c) => c.copy(input = v))
    opt[String]("output")
      .valueName("<output-path>")
      .text("s3 path to the output")
      .required()
      .action((v, c) => c.copy(output = v))
    opt[String]("schema-registry-url")
      .valueName("<schema-registry-url>")
      .text("URL of confluent schema registry")
      .required()
      .action((v, c) => c.copy(schemaRegistryUrl = v))
    opt[String]("schema-registry-subject")
      .valueName("<schema-registry-subject>")
      .text("Schema Registry Subject")
      .required()
      .action((v, c) => c.copy(schemaRegistrySubject = v))
    opt[Boolean]("flush-input")
      .valueName("<flush-input>")
      .text("If set will flush the files in the given input path.")
      .optional()
      .action((v, c) => c.copy(flushInput = v))
    opt[Boolean]("override-output")
      .valueName("<override-output>")
      .text("If set will override the files in the given output path should a conflict exist.")
      .optional()
      .action((v, c) => c.copy(overrideOutput = v))
    opt[String]("primary-key")
      .valueName("<primary-key>")
      .text("primary key of record used to compaction")
      .optional()
      .action((v, c) => c.copy(primaryKey = v))
    opt[String]("timestamp-key")
      .valueName("<timestamp-key>")
      .text("timestamp key of record used to compare for compaction")
      .optional()
      .action((v, c) => c.copy(timestampKey = v))
    opt[Boolean]("daily-run")
      .valueName("<daily-run>")
      .text("If compact avro files of previous day")
      .optional()
      .action((v, c) => c.copy(isDaylyRun = v))
    opt[Boolean]("monthly-run")
      .valueName("<monthly-run>")
      .text("If compact avro files of previous month")
      .optional()
      .action((v, c) => c.copy(isMonthlyRun = v))
  }

  def parse(args: Seq[String]): JobConfig = {
    parser.parse(args, JobConfig())
      .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
  }
}
