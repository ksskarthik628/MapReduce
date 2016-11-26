package assignment5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	
	/*
	 * This is the driver class - the main class - for the MapReduce program. The main function of the class
	 * takes three arguments as inputs - the input folder location, the output folder location and a string
	 * "rowByColumn" or "columnByRow" to indicate which method or partitioning should be run. It first runs
	 * a MapReduce job to read the data from .bz2 files located in the input folder, parsing the files and 
	 * creates an adjacency list representation for the input data. This process is the same as in Assignment
	 * 3. The output from this job was not available to me so I replicated this again. The output is then
	 * processed for use in PageRank calculation using matrix representation. Each step in the pre-processing
	 * is explained at their respective loops
	 */

	public static void main(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.out.println("Please provide three arguments, input directory, output directory and rowByColumn or columnByRow respectively.");
			return;
		}
		
		int runs = 10; // change to alter number of Page Rank loops
		
		// objects necessary for all runs
		Configuration[] conf = new Configuration[(2 * runs) + 6];
		Job[] job = new Job[(2 * runs) + 6];
		String count = null;
		
		/*
		 * loop 0 - create adjacency list representation
		 * loop 1 - create unique labels for all valid pages (the ones that occur as keys in adjacency list)
		 * loop 2 - gather a list of all dangling nodes in the adjacency list and store their labels
		 * loop 3 - create the matrix representation of adjacency list with labels instead of page names
		 * loop 4 - create initial ranks for every valid page based on their labels
		 */
		
		for (int i = 0; i < 5; i++) {
			
			switch(i) {
			
			/*
			 * loop 0	-	create adjacency list representation - Map only job
			 * input	-	folder with .bz2 files
			 * output	-	adjacency list
			 */
			case 0:
				conf[i] = new Configuration();
				job[i] = Job.getInstance(conf[i], "Adjacency List");
				job[i].setJarByClass(Driver.class);
				job[i].setMapperClass(ReaderMapper.class);
				job[i].setNumReduceTasks(0);
				job[i].setOutputKeyClass(Text.class);
				job[i].setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job[i], new Path(args[0]));
				FileOutputFormat.setOutputPath(job[i], new Path(args[1] + "_" + i));
				break;
				
			/*
			 * loop 1	-	create unique labels for all valid pages (the ones that occur as keys in adjacency list)
			 * 				single reducer to ensure unique labels for each page
			 * input	-	adjacency list
			 * output	-	unique labels for every valid page
			 * comments	-	this list is used for weeding out ghost nodes
			 */
			case 1:
				if (job[i - 1].waitForCompletion(true)) {
					// store count of number of valid pages
					count = Long.toString(job[i - 1].getCounters().findCounter(COUNTER.COUNT).getValue());
					conf[i] = new Configuration();
					job[i] = Job.getInstance(conf[i], "Label List");
					job[i].setJarByClass(Driver.class);
					job[i].setMapperClass(LabelMapper.class);
					job[i].setReducerClass(LabelReducer.class);
					job[i].setNumReduceTasks(1);
					job[i].setMapOutputKeyClass(NullWritable.class);
					job[i].setOutputKeyClass(Text.class);
					job[i].setOutputValueClass(Text.class);
					FileInputFormat.addInputPath(job[i], new Path(args[1] + "_0"));
					FileOutputFormat.setOutputPath(job[i], new Path(args[1] + "_" + i));
				}
				break;
				
			/*
			 * loop 2	-	gather a list of all dangling nodes in the adjacency list and store their labels - Map only job
			 * input	-	adjacency list
			 * cache	-	labels for valid pages
			 * output	-	list of labels indicating dangling nodes
			 */
			case 2:
				if (job[i - 1].waitForCompletion(true)) {
					conf[i] = new Configuration();
					conf[i].set("LABELS", args[1] + "_1");
					job[i] = Job.getInstance(conf[i], "Dangling List");
					job[i].setJarByClass(Driver.class);
					job[i].setMapperClass(DanglingMapper.class);
					job[i].setNumReduceTasks(0);
					job[i].setOutputKeyClass(Text.class);
					job[i].setOutputValueClass(NullWritable.class);
					FileInputFormat.addInputPath(job[i], new Path(args[1] + "_0"));
					FileOutputFormat.setOutputPath(job[i], new Path(args[1] + "_" + i));
				}
				break;
				
			/*
			 * loop 3	-	create the matrix representation of adjacency list with labels instead of page names - Map only job
			 * input	-	adjacency list
			 * cache	-	labels for valid pages
			 * output	-	sparse matrix representation of adjacency list with labels instead of page names
			 * comments	-	ghost nodes are weeded out
			 */
			case 3:
				if (job[i - 1].waitForCompletion(true)) {
					conf[i] = new Configuration();
					conf[i].set("LABELS", args[1] + "_1");
					job[i] = Job.getInstance(conf[i], "Matrix");
					job[i].setJarByClass(Driver.class);
					job[i].setMapperClass(MatrixMapper.class);
					job[i].setNumReduceTasks(0);
					job[i].setOutputKeyClass(Text.class);
					job[i].setOutputValueClass(Text.class);
					FileInputFormat.addInputPath(job[i], new Path(args[1] + "_0"));
					FileOutputFormat.setOutputPath(job[i], new Path(args[1] + "_" + i));
				}
				break;
				
			/*
			 * loop 4	-	create initial ranks for every valid page based on their labels - Map only job
			 * input	-	labels for valid pages
			 * output	-	initial page ranks for page rank calculation
			 */
			case 4:
				if (job[i - 1].waitForCompletion(true)) {
					conf[i] = new Configuration();
					conf[i].set("COUNT", count);
					job[i] = Job.getInstance(conf[i], "Ranker");
					job[i].setJarByClass(Driver.class);
					job[i].setMapperClass(RankerMapper.class);
					job[i].setNumReduceTasks(0);
					job[i].setOutputKeyClass(Text.class);
					job[i].setOutputValueClass(DoubleWritable.class);
					FileInputFormat.addInputPath(job[i], new Path(args[1] + "_1"));
					FileOutputFormat.setOutputPath(job[i], new Path(args[1] + "_" + i));
				}
				break;
			}
		}
		
		// if the requested run is a row by column partition
		if (args[2].equals("rowByColumn")) {
		
			/*
			 * Run the PageRank main algorithm for as many times as specified by the "runs" variable.
			 * 
			 * input	-	sparse matrix representation of adjacency list
			 * output	-	new page rank after iteration
			 * cache	-	previous page ranks for all nodes
			 * 				list of dangling nodes for dangling node contribution
			 */
			
			for (int i = 5; i < runs + 5; i++) {
				if (job[i - 1].waitForCompletion(true)) {
					conf[i] = new Configuration();
					conf[i].set("COUNT", count);
					conf[i].set("RANKS", args[1] + "_" + (i - 1));
					conf[i].set("DANGLING", args[1] + "_2");
					job[i] = Job.getInstance(conf[i], "Page Rank");
					job[i].setJarByClass(Driver.class);
					job[i].setMapperClass(PageRankMapper.class);
					job[i].setReducerClass(PageRankReducer.class);
					job[i].setMapOutputKeyClass(Text.class);
					job[i].setMapOutputValueClass(Text.class);
					job[i].setOutputKeyClass(Text.class);
					job[i].setOutputValueClass(DoubleWritable.class);
					FileInputFormat.addInputPath(job[i], new Path(args[1] + "_3"));
					FileOutputFormat.setOutputPath(job[i], new Path(args[1] + "_" + i));
				}
			}
			
			/*
			 * After "runs" loops of PageRank is done, give the Top K pages with ranks
			 * 
			 * input	-	final page rank after loops
			 * output	-	top k pages with their ranks
			 * cache	-	labels for valid pages
			 */
			
			if (job[runs + 4].waitForCompletion(true)) {
				conf[runs + 5] = new Configuration();
				conf[runs + 5].set("LABELS", args[1] + "_1");
				job[runs + 5] = Job.getInstance(conf[runs + 5], "TopK");
				job[runs + 5].setJarByClass(Driver.class);
				job[runs + 5].setMapperClass(TopKMapper.class);
				job[runs + 5].setReducerClass(TopKReducer.class);
				job[runs + 5].setNumReduceTasks(1);
				job[runs + 5].setOutputKeyClass(NullWritable.class);
				job[runs + 5].setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job[runs + 5], new Path(args[1] + "_" + (runs + 4)));
				FileOutputFormat.setOutputPath(job[runs + 5], new Path(args[1]));
			}
			
			System.exit(job[runs + 5].waitForCompletion(true) ? 0 : 1);
		}
		
		// if requested run is a column by row partition
		if (args[2].equals("columnByRow")) {
			
			int i = 5;
			
			/*
			 * Run the main PageRank algorithm for as many times as specified by the "runs" variable
			 */
			
			while (i < (2 * runs) + 5) {
				
				/*
				 * input	-	sparse matrix representation of adjacency list
				 * output	-	contribution output for each node
				 * cache	-	previous page rank of all nodes
				 */
				
				if (job[i - 1].waitForCompletion(true)) {
					conf[i] = new Configuration();
					conf[i].set("RANKS", args[1] + "_" + (i - 1));
					job[i] = Job.getInstance(conf[i], "Page Rank");
					job[i].setJarByClass(Driver.class);
					job[i].setMapperClass(PageRankColMapper.class);
					job[i].setReducerClass(PageRankColReducer.class);
					job[i].setMapOutputKeyClass(Text.class);
					job[i].setMapOutputValueClass(Text.class);
					job[i].setOutputKeyClass(Text.class);
					job[i].setOutputValueClass(DoubleWritable.class);
					FileInputFormat.addInputPath(job[i], new Path(args[1] + "_3"));
					FileOutputFormat.setOutputPath(job[i], new Path(args[1] + "_" + i));
					i++;
				}
				
				/*
				 * input	-	contribution for each node
				 * output	-	new page rank after iteration
				 * cache	-	previous page ranks for all nodes
				 * 				list of dangling nodes for dangling node contribution
				 */
				
				if (job[i - 1].waitForCompletion(true)) {
					conf[i] = new Configuration();
					conf[i].set("COUNT", count);
					conf[i].set("RANKS", args[1] + "_" + (i - 2));
					conf[i].set("DANGLING", args[1] + "_2");
					job[i] = Job.getInstance(conf[i], "Page Rank Aggregate");
					job[i].setJarByClass(Driver.class);
					job[i].setMapperClass(PageRankAggrMapper.class);
					job[i].setReducerClass(PageRankAggrReducer.class);
					job[i].setOutputKeyClass(Text.class);
					job[i].setOutputValueClass(DoubleWritable.class);
					FileInputFormat.addInputPath(job[i], new Path(args[1] + "_" + (i - 1)));
					FileOutputFormat.setOutputPath(job[i], new Path(args[1] + "_" + i));
					i++;
				}
			}
			
			/*
			 * After "runs" loops of PageRank is done, give the Top K pages with ranks
			 * 
			 * input	-	final page rank after loops
			 * output	-	top k pages with their ranks
			 * cache	-	labels for valid pages
			 */
			
			if (job[i - 1].waitForCompletion(true)) {
				conf[i] = new Configuration();
				conf[i].set("LABELS", args[1] + "_1");
				job[i] = Job.getInstance(conf[i], "TopK");
				job[i].setJarByClass(Driver.class);
				job[i].setMapperClass(TopKMapper.class);
				job[i].setReducerClass(TopKReducer.class);
				job[i].setNumReduceTasks(1);
				job[i].setOutputKeyClass(NullWritable.class);
				job[i].setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job[i], new Path(args[1] + "_" + (i - 1)));
				FileOutputFormat.setOutputPath(job[i], new Path(args[1]));
			}
			
			System.exit(job[i].waitForCompletion(true) ? 0 : 1);
			
		}
					
	}

}
