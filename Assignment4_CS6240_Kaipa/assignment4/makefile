spark.root=spark-submit

local.input=input

jar.name=target/scala-2.11/assignment4_2.11-1.0.jar

job.name=SparkPageRank

local.output=output

local.iterations=10

jar:
	sbt package

clean:
	sbt clean
	rm -rf target
	rm -rf ${local.output}

pagerank: clean jar
	${spark.root} --class ${job.name} --master local[*] ${jar.name} ${local.input} ${local.output} ${local.iterations}