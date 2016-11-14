# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=hadoop
local.input=input
jar.name=target/HadoopFIrst-0.0.1-SNAPSHOT.jar
job.name=hw01.WordCount
local.output=output

# Compiles code and builds jar.
jar:
	mvn install

# Removes local output directory.
clean:
	mvn clean
	rm -rf ${local.output}

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
local: clean jar
	${hadoop.root} jar ${jar.name} ${job.name} ${local.input} ${local.output}