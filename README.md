# spark-avro-compactor

Extend of spark job for compacting avro files together

Extend feature:
 - Allow to compact s3 avro files by primary field and timestamp field

## Running locally 


1. `local=true sbt`
2. `runMain ie.ianduffy.spark.avro.compactor.Runner --input inputPath --output outputPath --flush-input false --override-output false outputPath --schema-registry-url https://confluent-schema-registry --schema-registry-subject schema-subject --primary-key primary_key_field --timestamp-key timestamp_key_field` 

## Create all-in-one jar file

```
    sbt assembly
```

### Notice

1. Uncomment line ` "com.amazonaws" % "aws-java-sdk-core" % "1.10.6",` if missing library (build.sbt)
2. Update aws credential in function `runLocally` (Runner.scala)
3. Input and output path should be `s3n://` as `runLocally` config are using `fs.s3n`

## Arguments

--input <input-path>    The s3-path to the input
                       
--output <output-path>  The s3-path to the output
                       
--schema-registry-url <schema-registry-url> URL of confluent schema registry
                       
--schema-registry-subject <schema-registry-subject> Schema Registry Subject
                       
--flush-input <flush-input>   If set will flush the files in the given input path.
                       
--override-output <override-output>   If set will override the files in the given output path should a conflict exist (default: true).

--primary-key <primary-key>   primary key of record used to compaction
                       
--timestamp-key <timestamp-key>  timestamp key of record used to compare for compaction
                       
--daily-run <daily-run>  If compact avro files of previous day

--monthly-run <monthly-run>  If compact avro files of previous month

## Contributors 

1. [NGL](https://github.com/ngl91)


## Contributors of original spark-avro-compactor

 1. [Kevin Eid](https://github.com/kevllino)
 2. [Peter Barron](https://github.com/pbarron)
 3. [Ian Duffy](https://github.com/imduffy15)
