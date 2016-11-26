package assignment5;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

@SuppressWarnings("unused")
public class PageRankColReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	
	/*
	 * This is the PageRankColReducer reducer task that gets from the mapper, for every column,
	 * all the contributions it sends to various rows. For looking up the previous
	 * ranks for the columns, it stores in cache the ranks from before this iteration.
	 */
	
	private HashMap<String, Double> map = new HashMap<String, Double>();
	private double dangling = 0.0;
	private double count = 0.0;
	private static double randomness = 0.15;
	
	public void setup(Context context) throws IOException, InterruptedException {
		
		// read the ranks into cache and store them in a map
		try{
			String fileName = context.getConfiguration().get("RANKS");
            FileSystem fs = FileSystem.get(URI.create(fileName), context.getConfiguration());
            FileStatus[] status = fs.listStatus(new Path(fileName));
            for (int i = 0; i < status.length; i++){
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                    String line = br.readLine();
                    while (line != null){
                            String[] entry = line.split("\\t");
                            map.put(entry[0], Double.parseDouble(entry[1]));
                            line = br.readLine();
                    }
                    br.close();
            }
	    } catch(Exception e){
	            System.out.println("File not found");
	    }		
	}
		
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// for every row in the values, emit the row and the rank contribution it gets from the key (column).
		double prevRank = map.get(key.toString());
		for (Text value : values) {
			String[] link = value.toString().split("\\t");
			double con = Double.parseDouble(link[1]);
			if (con != 0.0)
				context.write(new Text(link[0]), new DoubleWritable(prevRank / con));
			else
				context.write(new Text(link[0]), new DoubleWritable(0.0));
			
		}
		
	}
	
}