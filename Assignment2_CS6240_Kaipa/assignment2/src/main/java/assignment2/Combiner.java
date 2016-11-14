package assignment2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class Combiner {
	
	/*
	 * This is the Combiner MapReduce class. This class has all the necessary classes for Map, Reduce, and Combiner
	 * that are necessary for running along with the Combiner class for aggregating intermediate results.
	 * This class is self contained and doesn't require any other class/file.
	 * This implementation employs a combiner to off-load aggregation at the reducer.
	 * The input of the mapper is taken as Text from the input file(s) and it outputs a key
	 * and value pair of types Text which contains the stationID and a custom value class called TwoDoubleWritable
	 * which holds what kind the temperature is (TMIN or TMAX), the temperature value itself and the number of records
	 * aggregated to get this temperature value. The Combiner takes as input a key and value pair of type Text and 
	 * TwoDoubleWritable which the mapper emitted, does an intermediate aggregation and outputs the same key as received
	 * with a new TwoDoubleWritable as output after aggregation. The Reducer takes
	 * a key and value pair of Text and TwoDoubleWritable and performs an aggregation before outputting the stationID
	 * and the corresponding mean min and mean max temperatures as Text.
	 */

	public static class TwoDoubleWritable implements Writable {
		
		/*
		 * This is the custom value class that is utilized throughout the entire MapReduce program. It implements the
		 * Writable interface and has override functions for readFields and write which are essential to complete the
		 * class definition. Each object of this class stores the type of temperature (TMIN or TMAX), the value
		 * of the temperature and the number of records aggregated to get this temperature value.
		 * It also provides setters, getters and toString functions for the three fields for ease of use.
		 */
		
		private String type = "";
		private double tem = 0;
		private double count = 1;

		@Override
		public void readFields(DataInput arg0) throws IOException {
			type = WritableUtils.readString(arg0);
			tem = arg0.readDouble();
			count = arg0.readDouble();
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			WritableUtils.writeString(arg0, type);
			arg0.writeDouble(tem);
			arg0.writeDouble(count);
		}
		
		public TwoDoubleWritable() {}
		
		public TwoDoubleWritable(String st, double t, double cou) {
			type = st;
			tem = t;
			count = cou;
		}
		
		public String getType() {
			return type;
		}
		
		public double getTemp() {
			return tem;
		}
		
		public double getCount() {
			return count;
		}
		
		public void setType(String t) {
			type = t;
		}
		
		public void setTemp(double t) {
			tem = t;
		}
		
		public void setCount(double t) {
			count = t;
		}
		
		public String toString() {
			return type + "\t" + tem + "\t" + count;
		}
		
	}

	public static class CombinerMapper extends Mapper<Object, Text, Text, TwoDoubleWritable>{
		
		/*
		 * This is the mapper class for the MapReduce program that takes in lines of text from the input file(s) as
		 * Text, parses the input accordingly and outputs key and value pairs of Text (which contains the stationID) and
		 * TwoDoubleWritable (which contains the type of temperature, the value of the temperature and the number of records 
		 * responsible for the temperature value) respectively. There is no local aggregation of any kind.
		 */
	
		private TwoDoubleWritable tem = new TwoDoubleWritable();
		private Text word = new Text();
	
		/*
		 * This is the main map function which reads a line of Text from input file(s) at a time, parses the line as a String,
		 * creates a TwoDoubleWritable object with the type of temperature, the value of the temperature and number of records
		 * and emits this key and value pair. There is no local aggregation of any kind. 
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
	
	public static class CombinerCombiner extends Reducer<Text, TwoDoubleWritable, Text, TwoDoubleWritable> {
		
		/*
		 * This is the Combiner class for the MapReduce program. This class takes as input a key and value pair or types Text and
		 * TwoDoubleWritable and outputs a key and value pair of Text and TwoDoubleWritable. This class is responsible for intermediate 
		 * aggregation of temperatures before they are sent to the reducer for final aggregation and output. The functionality is
		 * more or less the same as that of reducer.
		 */
		
		private TwoDoubleWritable result = new TwoDoubleWritable();
		
		/*
		 * This is the main function that performs intermediate aggregation based on the data emitted by the mapper. The footprint
		 * of the function is more of less the same as that of reducer but it does not output Text and Text as key and value. Instead,
		 * it emits Text as key and TwoDoubleWritable as value so that reducer gets the right kind of inputs. The function also 
		 * updates the number of records it aggregated locally to the output TwoDoubleWritable so that the reducer gets an accurate 
		 * mean min and mean max output.
		 */
		
		public void reduce(Text key, Iterable<TwoDoubleWritable> values, Context context) throws IOException, InterruptedException {
			double tminCount = 0;
			double tmaxCount = 0;
			double tmin = 0;
			double tmax = 0;
			
			for (TwoDoubleWritable val : values) {
				
				if (val.getType().equals("TMAX")) {
					tmax += val.getTemp();
					tmaxCount += val.getCount();
				} else if (val.getType().equals("TMIN")) {
					tmin += val.getTemp();
					tminCount += val.getCount();
				}
			}
			
			if (tmaxCount != 0) {
				result.setType("TMAX");
				result.setTemp(tmax);
				result.setCount(tmaxCount);
				context.write(key, result);
			}
			
			if (tminCount != 0) {
				result.setType("TMIN");
				result.setTemp(tmin);
				result.setCount(tminCount);
				context.write(key, result);
			}
			
		}
	}
	
	public static class CombinerReducer extends Reducer<Text, TwoDoubleWritable, Text, Text> {
		
		/*
		 * This is the reducer class for the MapReduce program that takes in the key and value pairs of Text and TwoDoubleWritable
		 * that the mapper and combiner emitted during the map stage of the execution. Since there is a combiner involved, the
		 * reducer does not receive every entry in the input file(s) but a few aggregated records as well and parses them one after
		 * the other. The values are grouped according to the key which is the stationID.
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
					tmaxCount += val.getCount();
				} else if (val.getType().equals("TMIN")) {
					tmin += val.getTemp();
					tminCount += val.getCount();
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
		Job job = Job.getInstance(conf, "combiner");
		job.setJarByClass(Combiner.class);
		job.setMapperClass(CombinerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TwoDoubleWritable.class);
		job.setCombinerClass(CombinerCombiner.class);
		job.setReducerClass(CombinerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
