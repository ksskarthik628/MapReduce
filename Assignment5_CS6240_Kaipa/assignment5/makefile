hadoop.root=hadoop

local.input=input

jar.name=target/assignment5-0.0.1-SNAPSHOT.jar

job.name=assignment5.Driver

local.output=output

jar:
	mvn install

clean:
	mvn clean
	rm -rf ${local.output}*

pagerankrc: clean jar
	${hadoop.root} jar ${jar.name} ${job.name} ${local.input} ${local.output} rowByColumn

pagerankcr: clean jar
	${hadoop.root} jar ${jar.name} ${job.name} ${local.input} ${local.output} columnByRow