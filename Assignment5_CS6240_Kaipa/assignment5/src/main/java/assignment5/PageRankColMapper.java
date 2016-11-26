package assignment5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class PageRankColMapper extends Mapper<Object, Text, Text, Text> {

	/*
	 * This is the PageRankColMapper map task for column by row jobs. The mapper reads the sparse matrix
	 * and emits the column label as key and (row, outputsForColumn) as value for the reducer.
	 */
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] line = value.toString().split("\\t");
		context.write(new Text(line[1]), new Text(line[0] + "\t" + line[2]));
		
	}
	
}