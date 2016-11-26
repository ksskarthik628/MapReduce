package assignment5;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class MatrixMapper extends Mapper<Object, Text, Text, Text> {
	
	/*
	 * This is the MatrixMapper that creates a sparse matrix representation of the adjacency list.
	 * The map task gets input an entry from the adjacency list and stores labels in cache so that
	 * it can emit only the labels and not the node names themselves. This also enables the task to
	 * weed out any ghost nodes by trying to look them up in the map of labels. This also helps in
	 * getting the actual contributions of pages and not lose any into ghost nodes. The task emits
	 * the sparse matrix in this format: (adjacentNode, (node, #adjacentNodes))
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
		
		String[] line = value.toString().split("\\t");
        String page = line[0];
        int contrib = 0;
        
        // count the number of real pages in adjacency list
        for (int i = 1; i < line.length; i++) {
        	if (map.containsKey(line[i]))
        		contrib++;
        }
        // emit only for real pages in adjacency list
		for (int i = 1; i < line.length; i++) {
			if (map.containsKey(line[i]))
				context.write(new Text(map.get(line[i])), new Text(map.get(page) + "\t" + contrib));
		}
		
		// emit ghost link from node to node so that nodes with no in-link are not lost
		context.write(new Text(map.get(page)), new Text(map.get(page) + "\t" + 0));
		
	}
	
}