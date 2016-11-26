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
public class PageRankAggrReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	/*
	 * This is the PageRankAggrReducer reducer task that gets from the mapper, for every row,
	 * all the contributions it receives from various columns. For looking up the previous
	 * ranks for the columns, it stores in cache the ranks from before this iteration. It
	 * also stores in cache the list of dangling nodes to calculate the dangling node
	 * contributions for page rank.
	 */
	
	private HashMap<String, Double> map = new HashMap<String, Double>();
	private HashSet<String> set = new HashSet<String>();
	private double dangling = 0.0;
	private double count = 0.0;
	private static double randomness = 0.15;
	
	public void setup(Context context) throws IOException, InterruptedException {
		
		// read the list of dangling nodes into cache and store them in a set
		try{
			String fileName = context.getConfiguration().get("DANGLING");
            FileSystem fs = FileSystem.get(URI.create(fileName), context.getConfiguration());
            FileStatus[] status = fs.listStatus(new Path(fileName));
            for (int i = 0; i < status.length; i++){
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                    String line = br.readLine();
                    while (line != null){
                    	set.add(line);
                        line = br.readLine();
                    }
                    br.close();
            }
	    } catch(Exception e){
	            System.out.println("File not found");
	    }
		
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
		
		
		for (String dang : set) {
			dangling += map.get(dang);
		}
		
		// calculate the dangling node contribution to be used in reduce function
		count = Double.parseDouble(context.getConfiguration().get("COUNT")); 
		
	}
		
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		// calculate the new page rank for each key (node) and emit it		
		double contribs = 0.0;
		for (DoubleWritable partial : values) {
			contribs += partial.get();
		}
		double rank = (randomness / count) + ((1 - randomness) * (contribs + (dangling / count)));
		context.write(key, new DoubleWritable(rank));
		
	}
	
}