package assignment3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.compress.BZip2Codec;

@SuppressWarnings("unused")
public class Driver {
	
	/*
	 * This is the driver class - the main class - of the MapReduce program. The driver function takes in two arguments,
	 * one for input file location and one for output file location. It first runs a MapReduce job to create an adjacency
	 * list of all web pages in the input folder (input files are to be provided as .bz2 files). The output of the first job
	 * is all pages with a default page rank of 1.0 and it's associated adjacency list. The output files are stored in a temporary
	 * folder, which is used as input for the next MapReduce job. The next 10 MapRedduce tasks are the meat of Page Rank
	 * formation. Each of the 10 jobs takes as input files with page name, corresponding page rank and adjacency list as input
	 * and performs Page Rank calculation based on the formula discussed in class, also taking care of dangling nodes. The final
	 * MapReduce job run by the driver is the top k MapReduce job.
	 * 
	 * Dangling nodes:
	 * Every node which is a dangling node updates it's page rank to a counter during the map phase of the MapReduce job.
	 * This counter is accessed once the job is completed and sent to the next MapReduce job along with the Configuration
	 * for the job. The next job accesses this delta from context in the map phase and updates every page rank with this delta
	 * to get corrected page rank before continuing with it's map and reduce tasks.
	 * 
	 * Top K:
	 * Every mapper in the map phase of this MapReduce job creates a local map of all pages and corresponding page rank values
	 * as key and value pairs and on clean up, sorts this map on the values to get all the entries sorted based on page rank and
	 * outputs only the top k (100 in this case). The output key of the mapper is a null and the value is the page and it's rank.
	 * Since all the keys are the same, all pages and their page ranks are sent to a single reduce task (which is also forced to be
	 * 1 by the driver). This ensures that there is a global top k and only one output file. The reducer also performs a similar
	 * task like the mapper and finally outputs top k page ranks in the entire MapReduce program.
	 * 
	 * Page Rank idea courtesy : http://blog.xebia.com/wiki-pagerank-with-hadoop/
	 * HashMap value based sorting idea courtesy : http://stackoverflow.com/questions/109383/sort-a-mapkey-value-by-values-java
	 */

	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.println("Please provide two arguments, input directory and output directory respectively.");
			return;
		}
		
		int runs = 10; // change to alter number of Page Rank loops
		
		// objects necessary for all runs
		Configuration[] conf = new Configuration[runs + 2];
		Job[] job = new Job[runs + 2];
		Counters[] counters = new Counters[runs + 2];
		
		// initialize job 0, which is parsing the bz2 files and creating the adjacency list
		// this is a map only task so identity reducer is used by default
		conf[0] = new Configuration();
		job[0] = Job.getInstance(conf[0], "Adjacency List");
		job[0].setJarByClass(Driver.class);
		job[0].setMapperClass(ReaderMapper.class);
		job[0].setOutputKeyClass(Text.class);
		job[0].setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job[0], new Path(args[0]));
		FileOutputFormat.setOutputPath(job[0], new Path(args[1] + "_0"));
		
		// loop for running all the Page Rank jobs
		for (int i = 1; i < runs + 1; i++) {
			// for every consecutive job, wait for it's previous job to finish
			if (job[i - 1].waitForCompletion(true)) {
				counters[i - 1] = job[i - 1].getCounters();
				// get number of node count and dangling node page rank summation to be used in next job
				String count = Long.toString(counters[i - 1].findCounter(DANGLING_COUNTER.COUNT).getValue());
				String delta = Long.toString(counters[i - 1].findCounter(DANGLING_COUNTER.DELTA).getValue());
				conf[i] = new Configuration();
				// set the collected count and delta in configuration for next job and run it
				conf[i].set("COUNT", count);
				conf[i].set("DELTA", delta);
				job[i] = Job.getInstance(conf[i], "Page Rank " + i);
				job[i].setJarByClass(Driver.class);
				job[i].setMapperClass(PageRankMapper.class);
				job[i].setReducerClass(PageRankReducer.class);
				job[i].setOutputKeyClass(Text.class);
				job[i].setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job[i], new Path(args[1] + "_" + (i - 1)));
				FileOutputFormat.setOutputPath(job[i], new Path(args[1] + "_" + i));
			}
		}
		
		// wait for the last Page Rank job to finish before running the Top K job
		if (job[runs].waitForCompletion(true)) {
			counters[runs] = job[runs].getCounters();
			// count and delta are required to update the final page ranks with dangling nodes of the very last Page Rank job
			String count = Long.toString(counters[runs].findCounter(DANGLING_COUNTER.COUNT).getValue());
			String delta = Long.toString(counters[runs].findCounter(DANGLING_COUNTER.DELTA).getValue());
			conf[runs + 1] = new Configuration();
			conf[runs + 1].set("COUNT", count);
			conf[runs + 1].set("DELTA", delta);
			job[runs + 1] = Job.getInstance(conf[runs + 1], "Top k");
			job[runs + 1].setJarByClass(Driver.class);
			job[runs + 1].setMapperClass(TopKMapper.class);
			job[runs + 1].setReducerClass(TopKReducer.class);
			job[runs + 1].setOutputKeyClass(NullWritable.class);
			job[runs + 1].setOutputValueClass(Text.class);
			job[runs + 1].setNumReduceTasks(1);
			FileInputFormat.addInputPath(job[runs + 1], new Path(args[1] + "_" + runs));
			FileOutputFormat.setOutputPath(job[runs + 1], new Path(args[1]));
			
			System.exit(job[runs + 1].waitForCompletion(true) ? 0 : 1);
		}
		
	}

}
