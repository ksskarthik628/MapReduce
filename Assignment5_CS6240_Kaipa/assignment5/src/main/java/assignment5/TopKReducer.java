package assignment5;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
	 * at the same reduce task.
	 */
	
	private HashMap<String, String> map = new HashMap<String, String>();
	
	public void setup(Context context) throws IOException, InterruptedException {
		
		// read the labels into cache and store them in a map
		try {
			String fileName = context.getConfiguration().get("LABELS");
			FileSystem fs = FileSystem.get(URI.create(fileName), context.getConfiguration());
			FileStatus[] status = fs.listStatus(new Path(fileName));
			for (int i = 0; i < status.length; i++) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line = null;
				while((line = br.readLine()) != null) {
					String[] idx = line.split("\\t");
					map.put(idx[1], idx[0]);
				}
				br.close();
			}
		} catch(Exception e) {
			System.out.println("File not found");
		}
	}
	
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
		
		for (Text value : values) {
			String[] v = value.toString().split("\\t");
			double rank = Double.parseDouble(v[1]);
			topk.put(v[0], rank);
			
		}
		
		// sort the map on it's value
		topk = (HashMap<String, Double>) MapUtil.sortByValue(topk);
		int count = 0;
		
		// output exactly the top k
		for (Map.Entry<String, Double> entry : topk.entrySet()) {
			context.write(NullWritable.get(), new Text(map.get(entry.getKey()) + "\t" + entry.getValue()));
			count++;
			if (count >= k) break;
		}
		
	}
	
}
