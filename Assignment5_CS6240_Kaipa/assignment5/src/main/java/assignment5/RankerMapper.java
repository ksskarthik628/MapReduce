package assignment5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class RankerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	
	/*
	 * This is the RankerMapper task that creates the initial page rank for each valid node with a
	 * value of 1 / #realNodes. The input for the task is the labels mapping since initial page rank
	 * doesn't depend on anything else.
	 */
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] line = value.toString().split("\\t");
        String page = line[1];
        double count = Double.parseDouble(context.getConfiguration().get("COUNT"));
		context.write(new Text(page), new DoubleWritable(1 / count));
		
	}
	
}