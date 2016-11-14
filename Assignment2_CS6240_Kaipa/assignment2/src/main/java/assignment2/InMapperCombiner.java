package assignment2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

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
public class InMapperCombiner {
	
	/*
	 * This is the InMapperCombiner MapReduce class. This class has all the necessary classes for Map, and Reduce
	 * that are necessary for running.
	 * This class is self contained and doesn't require any other class/file.
	 * This implementation employs an in-mapper combiner to off-load aggregation at the reducer.
	 * The input of the mapper is taken as Text from the input file(s). The lines are then parsed for the corresponding stationID
	 * and temperatures and are locally aggregated before they are sent out as a key and value pairs of Text and TwoDoubleWritable
	 * that contains the type of temperature it represents, the temperature value itself and the number of records it
	 * aggregated to get the value. The Reducer takes
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

	public static class InMapperCombinerMapper extends Mapper<Object, Text, Text, TwoDoubleWritable>{
		
		/*
		 * This is the mapper class for the MapReduce program that takes in lines of text from the input file(s) as
		 * Text, parses the input accordingly, locally aggregates the temperatures corresponding to a stationID 
		 * and outputs key and value pairs of Text (which contains the stationID) and
		 * TwoDoubleWritable (which contains the type of temperature, the value of the temperature and the number of records 
		 * responsible for the temperature value) respectively.
		 */
	
		private TwoDoubleWritable tem = new TwoDoubleWritable();
		private Text word = new Text();
		private HashMap<String, double[]> iMC;
	
		/*
		 * The setup function initializes the HashMap that the map function puts records into for aggregation locally.
		 */
		
		public void setup(Context context) {
			iMC = new HashMap<String, double[]>();
		}
		
		/*
		 * This is the main map function which reads a line of Text from input file(s) at a time, parses the line as a String,
		 * and stores or modifies the local temperature aggregation in a HashMap.
		 */
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			double[] temp = new double[4];
			if (record.contains("TMAX") || record.contains("TMIN")) {
				String[] st = record.split(",");
				if (!st[3].equals("")) {
					if (iMC.containsKey(st[0])) {
						temp = iMC.get(st[0]);
						if (st[2].equals("TMAX")) {
							temp[2] += Double.parseDouble(st[3]);
							temp[3] += 1;
						}
						else if (st[2].equals("TMIN")) {
							temp[0] += Double.parseDouble(st[3]);
							temp[1] += 1;
						}
					} else {
						temp = new double[4];
						if (st[2].equals("TMAX")) {
							temp[2] = Double.parseDouble(st[3]);
							temp[3] = 1;
						}
						else if (st[2].equals("TMIN")) {
							temp[0] = Double.parseDouble(st[3]);
							temp[1] = 1;
						}
					}
					iMC.put(st[0], temp);
					
				}
			}
		}
		
		/*
		 * The cleanup function iterates through all the entries in the local aggregation HashMap and emits a key and value
		 * pair of Text and TwoDoubleWritable with stationID and type of temperature, aggregate temperature, number of records respectively
		 */
		
		public void cleanup(Context context) throws IOException, InterruptedException  {
			Iterator<Entry<String, double[]>> it = iMC.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, double[]> pair = (Map.Entry<String, double[]>)it.next();
				String key = pair.getKey();
				double[] value = pair.getValue();
				tem.setType("TMIN");
				tem.setTemp(value[0]);
				tem.setCount(value[1]);
				word.set(key);
				context.write(word, tem);
				
				tem.setType("TMAX");
				tem.setTemp(value[2]);
				tem.setCount(value[3]);
				context.write(word, tem);
			}
		}
	}
	
	public static class InMapperCombinerReducer extends Reducer<Text, TwoDoubleWritable, Text, Text> {
		
		/*
		 * This is the reducer class for the MapReduce program that takes in the key and value pairs of Text and TwoDoubleWritable
		 * that the mapper emitted during the map stage of the execution. Since there is an in-mapper combiner involved, the
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
		Job job = Job.getInstance(conf, "in-mapper combiner");
		job.setJarByClass(InMapperCombiner.class);
		job.setMapperClass(InMapperCombinerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TwoDoubleWritable.class);
		job.setReducerClass(InMapperCombinerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
