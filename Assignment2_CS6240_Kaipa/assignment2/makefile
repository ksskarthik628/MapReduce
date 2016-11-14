hadoop.root=hadoop

local.input.combiner=inputCombiner
local.input.secondarysort=inputSecondarySort

jar.name=target/assignment2-0.0.1-SNAPSHOT.jar

job.nocombiner.name=assignment2.NoCombiner
job.combiner.name=assignment2.Combiner
job.inmappercombiner.name=assignment2.InMapperCombiner
job.secondarysort.name=assignment2.SecondarySort

local.output.combiner=outputCombiner
local.output.secondarysort=outputSecondarySort

jar:
	mvn install

cleancombiner:
	mvn clean
	rm -rf ${local.output.combiner}

cleansecondarysort:
	mvn clean
	rm -rf ${local.output.secondarysort}

nocombiner: cleancombiner jar
	${hadoop.root} jar ${jar.name} ${job.nocombiner.name} ${local.input.combiner} ${local.output.combiner}

combiner: cleancombiner jar
	${hadoop.root} jar ${jar.name} ${job.combiner.name} ${local.input.combiner} ${local.output.combiner}

inmappercombiner: cleancombiner jar
	${hadoop.root} jar ${jar.name} ${job.inmappercombiner.name} ${local.input.combiner} ${local.output.combiner}

secondarysort: cleansecondarysort jar
	${hadoop.root} jar ${jar.name} ${job.secondarysort.name} ${local.input.secondarysort} ${local.output.secondarysort}