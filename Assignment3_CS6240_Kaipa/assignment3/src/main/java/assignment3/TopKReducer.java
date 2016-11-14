package assignment3;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

@SuppressWarnings("unused")
public class TopKReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	
	/*
	 * This is the reducer class for Top K job in the MapReduce jobs. There is ever only one reducer task created by
	 * the driver function. Even if that were not the case, since all the output keys from the corresponding mapper
	 * are NullWritable objects, all the local top k pages will be associated with a single key and hence end up
	 * at the same reduce task. This class does exactly the same thing as it's corresponding mapper except update the
	 * page rank.
	 */
	
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		/*
		 * The reduce function gets only one key, a NullWritable object, and all it's values are the local top k pages
		 * from every mapper task created. The function puts them all in a map with page name and page rank as key and
		 * value. Once all the pages have been put into the map, the function sorts the map based on it's values (rank)
		 * and outputs exactly the top k pages. Since there is only one reduce task and only one key, the output of the
		 * reduce function is going to be guaranteed as the global top k ranked pages in the input files.
		 */
		
		// value of k
		int k = 100;
		
		// map for storing the page and it's page rank for local top k calculation
		HashMap<String, Double> topk = new HashMap<String, Double>();
		
		// for every page and rank Text object, get the page name, the rank and put it into the map
		for (Text value : values) {
			String v = value.toString();
			double rank = Double.parseDouble(v.split("\t")[0]);
			topk.put(v, rank);
			
		}
		
		// sort the map on it's value
		topk = (HashMap<String, Double>) MapUtil.sortByValue(topk);
		int count = 0;
		
		// output exactly the top k
		for (String t : topk.keySet()) {
			context.write(NullWritable.get(), new Text(t));
			count++;
			if (count >= k) break;
		}
		
	}
	
}
