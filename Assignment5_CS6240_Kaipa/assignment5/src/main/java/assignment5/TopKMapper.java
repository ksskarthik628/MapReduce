package assignment5;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class TopKMapper extends Mapper<Object, Text, NullWritable, Text> {
	
	/*
	 * This is the TopKMapper map task that reads all the final ranks for all nodes
	 * and emits only the top k pages to the reducer. This is the local top k.
	 */
	
	private HashMap<String, Double> topk = new HashMap<String, Double>();
	// value of k
	private int k = 100;
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] entry = value.toString().split("\\t");
		topk.put(entry[0], Double.parseDouble(entry[1]));
		        
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
			context.write(NullWritable.get(), new Text(entry.getKey() + "\t" + entry.getValue()));
			count++;
			if (count >= k) break;
		}
		
	}
	
}
