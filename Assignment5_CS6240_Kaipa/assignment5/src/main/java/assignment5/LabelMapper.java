package assignment5;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class LabelMapper extends Mapper<Object, Text, NullWritable, Text> {
	
	/*
	 * This is the LabelMapper that takes as input adjacency list and emits only the main node
	 * as value with a null as key to make sure only one reduce task handles the labeling.
	 */
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] line = value.toString().split("\\t");
        String page = line[0];
		context.write(NullWritable.get(), new Text(page));
		
	}
	
}