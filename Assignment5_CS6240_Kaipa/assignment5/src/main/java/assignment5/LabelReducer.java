package assignment5;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

@SuppressWarnings("unused")
public class LabelReducer extends Reducer<NullWritable, Text, Text, Text> {
	
	/*
	 * This is the LabelReducer that gets only one key, the null key as the only key and all valid pages
	 * as a list of values. Using a local counter, each valid node is assigned a unique label and emitted.
	 */
		
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int label = 0;
		
        for(Text value : values){
        	context.write(value, new Text(Integer.toString(label++)));
        }
 		
	}
	
}