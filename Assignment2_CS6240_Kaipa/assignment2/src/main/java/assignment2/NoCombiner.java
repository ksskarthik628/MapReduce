package assignment2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("unused")
public class NoCombiner {
	
	/*
	 * This is the No Combiner MapReduce class. This class has all the necessary classes for Map, and Reduce
	 * that are necessary for running. This class is self contained and doesn't require any other class/file.
	 * This implementation does not employ a combiner in any form and relies on aggregation at the reducer.
	 * The input of the mapper is taken as Text from the input file(s) and it outputs a key
	 * and value pair of types Text which contains the stationID and a custom value class called TwoDoubleWritable
	 * which holds what kind the temperature is (TMIN or TMAX) and the temperature value itself. The Reducer takes
	 * a key and value pair of Text and TwoDoubleWritable and performs an aggregation before outputting the stationID
	 * and the corresponding mean min and mean max temperatures as Text.
	 */
	
	public static class TwoDoubleWritable implements Writable {
		
		/*
		 * This is the custom value class that is utilized throughout the entire MapReduce program. It implements the
		 * Writable interface and has override functions for readFields and write which are essential to complete the
		 * class definition. Each object of this class stores the type of temperature (TMIN or TMAX) and also the value
		 * of the temperature. It also provides setters, getters and toString functions for the two fields for ease of use.
		 */
		
		private String type = "";
		private double tem = 0;

		@Override
		public void readFields(DataInput arg0) throws IOException {
			type = WritableUtils.readString(arg0);
			tem = arg0.readDouble();
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			WritableUtils.writeString(arg0, type);
			arg0.writeDouble(tem);
		}
		
		public TwoDoubleWritable() {}
		
		public TwoDoubleWritable(String st, double t) {
			type = st;
			tem = t;
		}
		
		public String getType() {
			return type;
		}
		
		public double getTemp() {
			return tem;
		}
		
		public void setType(String t) {
			type = t;
		}
		
		public void setTemp(double t) {
			tem = t;
		}
		
		public String toString() {
			return type + "\t" + tem;
		}
		
	}

	public static class NoCombinerMapper extends Mapper<Object, Text, Text, TwoDoubleWritable>{
		
		/*
		 * This is the mapper class for the MapReduce program that takes in lines of text from the input file(s) as
		 * Text, parses the input accordingly and outputs key and value pairs of Text (which contains the stationID) and
		 * TwoDoubleWritable (which contains the type of temperature and the value of the temperature) respectively.
		 * There is no local aggregation of any kind.
		 */
	
		private TwoDoubleWritable tem = new TwoDoubleWritable();
		private Text word = new Text();
	
		/*
		 * This is the main map function which reads a line of Text from input file(s) at a time, parses the line as a String,
		 * creates a TwoDoubleWritable object with the type of temperature and the value of the temperature and emits this key and
		 * value pair. There is no local aggregation of any kind. 
		 */
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			if (record.contains("TMAX") || record.contains("TMIN")) {
				String[] st = record.split(",");
				if (!st[3].equals("")) {
					tem.setType(st[2]);
					tem.setTemp(Double.parseDouble(st[3]));
					
					word.set(st[0]);
					context.write(word, tem);
				}
			}
		}
	}
	
	public static class NoCombinerReducer extends Reducer<Text, TwoDoubleWritable, Text, Text> {
		
		/*
		 * This is the reducer class for the MapReduce program that takes in the key and value pairs of Text and TwoDoubleWritable
		 * that the mapper emitted during the map stage of the execution. Since there is no combiner involved in any form, the
		 * reducer receives every entry in the input file(s) as is and parses them one after the other. The values are grouped
		 * according to the key which is the stationID.
		 */
		
		private Text result = new Text();
		
		/*
		 * This is the main reduce function which gets one key at a time with all values associated with that key. It iterates through
		 * all the values and aggregates the min and max temperatures and finally emits the stationID as Text and the mean min and mean max
		 * temperatures of the station as a Text as well. The stationIDs from a single reduce task will all be locally sorted.
		 */
		
		public void reduce(Text key, Iterable<TwoDoubleWritable> values, Context context) throws IOException, InterruptedException {
			double tminCount = 0;
			double tmaxCount = 0;
			double tmin = 0;
			double tmax = 0;
			
			for (TwoDoubleWritable val : values) {
				
				if (val.getType().equals("TMAX")) {
					tmax += val.getTemp();
					tmaxCount += 1;
				} else if (val.getType().equals("TMIN")) {
					tmin += val.getTemp();
					tminCount += 1;
				}
			}
			
			result.set(Math.round(tmin / tminCount * 100.0) / 100.0 + ", " + Math.round(tmax / tmaxCount * 100.0) / 100.0);
			context.write(key, result);
		}
	}
	
	/*
	 * This is the main function for the MapReduce program. This is where all the mapper and reducer classes are assigned
	 * along with any partitioners, combiners and comparators, if any.
	 */
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "no combiner");
		job.setJarByClass(NoCombiner.class);
		job.setMapperClass(NoCombinerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TwoDoubleWritable.class);
		job.setReducerClass(NoCombinerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
