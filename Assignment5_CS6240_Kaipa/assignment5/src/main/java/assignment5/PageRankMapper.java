package assignment5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class PageRankMapper extends Mapper<Object, Text, Text, Text> {
	
	/*
	 * This is the PageRankMapper map task for row by column jobs. The mapper reads the sparse matrix
	 * and emits the row label as key and (column, outputsForColumn) as value for the reducer.
	 */
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] line = value.toString().split("\\t");
		context.write(new Text(line[0]), new Text(line[1] + "\t" + line[2]));
		
	}
	
}