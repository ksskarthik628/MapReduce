hadoop.root=hadoop

local.input=input

jar.name=target/assignment3-0.0.1-SNAPSHOT.jar

job.name=assignment3.Driver

local.output=output

jar:
	mvn install

clean:
	mvn clean
	rm -rf ${local.output}*

pagerank: clean jar
	${hadoop.root} jar ${jar.name} ${job.name} ${local.input} ${local.output}