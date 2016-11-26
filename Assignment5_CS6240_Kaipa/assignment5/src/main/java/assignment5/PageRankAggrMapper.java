package assignment5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class PageRankAggrMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	
	/*
	 * This is the PageRankAggrMapper map task for column by row jobs. The mapper reads the partial
	 * ranks for each row element and emits the same with key as node label and value as partial rank
	 */
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] line = value.toString().split("\\t");
		double partialRank = Double.parseDouble(line[1]);
		context.write(new Text(line[0]), new DoubleWritable(partialRank));
		
	}
	
}