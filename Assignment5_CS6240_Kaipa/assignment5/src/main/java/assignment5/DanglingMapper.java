package assignment5;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class DanglingMapper extends Mapper<Object, Text, Text, NullWritable> {
	
	/*
	 * This is the DanglingMapper that emits the list of all dangling nodes in the adjacency list.
	 * Since the page rank algorithm only deals with labels, they are stored into cache during the
	 * setup phase. If any node in the adjacency list doesn't have any elements in the adjacency list,
	 * it is emitted as a dangling node.
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
					map.put(idx[0], idx[1]);
				}
				br.close();
			}
		} catch(Exception e) {
			System.out.println("File not found");
		}
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		// emit only the labels of those pages that have no nodes in their adjacency list
		String[] line = value.toString().split("\\t");
        String page = line[0];
        if (line.length == 1)
        	context.write(new Text(map.get(page)), NullWritable.get());
		
	}
	
}