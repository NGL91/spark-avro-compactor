# spark-avro-compactor

Extend of spark job for compacting avro files together

Extend feature:
 - Allow to compact avro file by primary field and timestamp field

## Running locally 

1. `local=true sbt`
2. `runMain ie.ianduffy.spark.avro.compactor.Runner -i inputPath --output  outputPath --schema-registry-url https://confluent-schema-registry --schema-registry-subject schema-subject --primary-key primary_key_field --timestamp-key timestamp_key_field` 

## Contributors 

1. [NGL](https://github.com/ngl91)


## Contributors of original spark-avro-compactor

 1. [Kevin Eid](https://github.com/kevllino)
 2. [Peter Barron](https://github.com/pbarron)
 3. [Ian Duffy](https://github.com/imduffy15)
