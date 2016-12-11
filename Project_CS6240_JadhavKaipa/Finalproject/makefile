spark.root=spark-submit

local.folder=input

local.partitions.input=10

jar.name=target/scala-2.11/finalproject_2.11-1.0.jar

job.name=Classification

jar:
	sbt package

clean:
	sbt clean

clean-training:
	rm -rf target
	rm -rf ${local.folder}/labelled_processed
	rm -rf ${local.folder}/model

clean-prediction:
	rm -rf target
	rm -rf ${local.folder}/unlabelled_processed
	rm -rf ${local.folder}/output

classification-training: clean clean-training jar
	${spark.root} --class ${job.name} --master local ${jar.name} ${local.folder} ${local.partitions.input} training

classification-prediction: clean clean-prediction jar
	${spark.root} --class ${job.name} --master local ${jar.name} ${local.folder} ${local.partitions.input} prediction