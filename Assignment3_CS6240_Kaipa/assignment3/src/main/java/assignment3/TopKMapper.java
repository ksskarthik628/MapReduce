package assignment3;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class TopKMapper extends Mapper<Object, Text, NullWritable, Text> {
	
	/*
	 * This is the mapper class for Top K job in the MapReduce jobs. This class accesses the output from
	 * the last Page Rank job, updates the ranks with the dangling nodes contribution and outputs only 
	 * the local top k (100 in this case) page ranked pages. To update the page rank, the class
	 * as access to COUNT and DELTA counters from the previous Page Rank job.
	 */

	// map for storing the page and it's page rank for local top k calculation
	private HashMap<String, Double> topk = new HashMap<String, Double>();
	// value of k
	private int k = 100;
	// randomness factor for all page rank calculations.
	private static final double randomness = 0.85;
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		/*
		 * This is the main map class for calculating local top k. The function gets as input the page name,
		 * it's page rank and it's adjacency list. It also has access to the DELTA and COUNT counters from
		 * the previous Page Rank job to update the corresponding page ranks with dangling node contributions.
		 * The function stores the page and page rank as key and value respectively for all the pages it sees
		 * before finally sorting the map based on the value (rank) and outputs only the top k that it has stored.
		 */
		
		int pageIndex = value.find("\t");
 
		// get page name
        String page = Text.decode(value.getBytes(), 0, pageIndex);
        
        // update page rank with dangling node contributions
        double previousDelta = Double.parseDouble(context.getConfiguration().get("DELTA")) / Math.pow(10.0, 12);
        double count = Double.parseDouble(context.getConfiguration().get("COUNT"));
        previousDelta /= count;
        double correctedRank = Double.parseDouble(value.toString().split("\t")[1]) + (randomness * previousDelta);
        
        // store the page and it's corrected rank in map
        topk.put(page, correctedRank);
        
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		/*
		 * This is the clean up function that sorts the map the mapper task has accumulated based on it's value
		 * (rank) and outputs exactly the top k ranked pages to the reducer.  The output key is a NullWritable 
		 * object so that every mapper task sends it's output to exactly one  reduce task (which is also set to 
		 * be one in driver function).
		 */
		
		// sort the map on it's value
		topk = (HashMap<String, Double>) MapUtil.sortByValue(topk);
		int count = 0;
		
		// output exactly the top k
		for (Map.Entry<String, Double> entry : topk.entrySet()) {
			context.write(NullWritable.get(), new Text(entry.getValue() + "\t" + entry.getKey()));
			count++;
			if (count >= k) break;
		}
		
	}
	
}
