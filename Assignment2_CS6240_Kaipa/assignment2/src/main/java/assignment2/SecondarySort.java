package assignment2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings({ "rawtypes", "unused" })
public class SecondarySort {
	
	/*
	 * This is the Secondary Sort class. This class has all the necessary classes for Map, Reduce, Partitioner,
	 * and Comparators that are necessary for running. This class is self contained and doesn't require any other class/file.
	 * The input of the mapper is taken as Text from the input file(s) and it outputs a key and value pair
	 * of types Text which contains the stationID and the year of the record and a custom value class called DataWritable which holds which year the data
	 * belongs to, what kind the temperature is (TMIN or TMAX) and the temperature value itself. The reducer takes a key and a 
	 * value pair of Text and DataWritable and performs an aggregation for each year's data for a single stationID and
	 * outputs the stationID and it's corresponding mean min and mean max values sorted based on the year. 
	 */
	
	public static class DataWritable implements Writable {
		
		/*
		 * This is the custom value class that is utilized throughout the entire MapReduce program. It implements the
		 * Writable interface and has override functions for readFields and write which are essential to complete the
		 * class definition. Each object of this class stores the type of temperature (TMIN or TMAX), the value
		 * of the temperature and the year that this temperature value corresponds to.
		 * It also provides setters, getters and toString functions for the three fields for ease of use.
		 */
		
		private String year = "";
		private String type = "";
		private double temperature = 0;

		@Override
		public void readFields(DataInput arg0) throws IOException {
			year = WritableUtils.readString(arg0);
			type = WritableUtils.readString(arg0);
			temperature = arg0.readDouble();			
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			WritableUtils.writeString(arg0, year);
			WritableUtils.writeString(arg0, type);
			arg0.writeDouble(temperature);
		}
		
		public DataWritable() {}
		
		public DataWritable(String y, String t, double tp) {
			year = y;
			type = t;
			temperature = tp;
		}
		
		public String getYear() {
			return year;
		}
		
		public String getType() {
			return type;
		}
		
		public double getTemperature() {
			return temperature;
		}
		
		public void setYear(String y) {
			year = y;
		}
		
		public void setType(String t) {
			type = t;
		}
		
		public void setTemperature(double t) {
			temperature = t;
		}
		
		public String toString() {
			return year + "\t" + type + "\t" + temperature;
		}
		
	}
	
	public static class SecondarySortMapper extends Mapper<Object, Text, Text, DataWritable> {
		
		/*
		 * This is the mapper class for the MapReduce program that takes in lines of text from the input file(s) as
		 * Text, parses the input accordingly and outputs key and value pairs of Text (which contains the stationID and year) and
		 * DataWritable (which contains the type of temperature, the value of the temperature, and the year) respectively.
		 * There is no local aggregation of any kind.
		 */
		
		private DataWritable out = new DataWritable();
		private Text word = new Text();
		
		/*
		 * This is the main map function which reads a line of Text from input file(s) at a time, parses the line as a String,
		 * creates a DataWritable object with the type of temperature, the value of the temperature, and the year and emits this key and
		 * value pair. There is no local aggregation of any kind. 
		 */
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			if (record.contains("TMAX") || record.contains("TMIN")) {
				String[] st = record.split(",");
				if (!st[3].equals("")) {
					out.setYear(st[1].substring(0, 4));
					out.setType(st[2]);
					out.setTemperature(Double.parseDouble(st[3]));
					
					word.set(st[0] + "_" + st[1].substring(0, 4));
					context.write(word, out);
				}
			}
		}
	}
	
	public static class SecondarySortPartitioner extends Partitioner<Text, DataWritable> {
		
		/*
		 * This is the partitioner class for the MapReduce program. This class extends the Partitioner class
		 * and has an override function getPartition that assigns for each key, which reduce task it needs to be sent to.
		 * This partitioning is achieved by using the hash code of the stationID that is available as a composite key
		 * along with the year.
		 */
		
		@Override
		public int getPartition(Text key, DataWritable value, int numReduceTasks) {
			String word[] = key.toString().split("_");
			return Math.abs(word[0].hashCode() % numReduceTasks);
		}
		
	}
	
	public static class SecondarySortKeyComparator extends WritableComparator {
		
		/*
		 * This is the Key Sort Comparator for the MapReduce program. This comparator decides which
		 * sorted order the keys should be sent to the reducer based on both the stationID and the year.
		 */
		
		protected SecondarySortKeyComparator() {
			super(Text.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text k1 = (Text) w1;
			Text k2 = (Text) w2;
			
			String[] s1 = k1.toString().split("_");
			String[] s2 = k2.toString().split("_");
			
			int stationCompare = s1[0].compareTo(s2[0]);
			if (stationCompare == 0) {
				return s1[1].compareTo(s2[1]);
			}
			return stationCompare;
		}
		
	}
	
	public static class SecondarySortGroupingComparator extends WritableComparator {
		
		/*
		 * This is the Grouping Comparator for the MapReduce program. This comparator decides which
		 * keys should be grouped together and be sent at once to the reducer based on the stationID alone.
		 */
		
		protected SecondarySortGroupingComparator() {
			super(Text.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text k1 = (Text) w1;
			Text k2 = (Text) w2;
			
			String[] s1 = k1.toString().split("_");
			String[] s2 = k2.toString().split("_");
			
			return s1[0].compareTo(s2[0]);
		}
		
	}
	
	public static class SecondarySortReducer extends Reducer<Text, DataWritable, Text, Text> {
		
		/*
		 * This is the reducer class for the MapReduce program that takes in the key and value pairs of Text and DataWritable
		 * that the mapper emitted during the map stage of the execution. Since there is a composite key and key sort as well as grouping
		 * comparators in play, the reduce task receives a single key based on the stationID and all the years' data associated
		 * with this stationID are received as values. The reduce iterates through all the values, calculates the mean min and mean max
		 * of all years individually and outputs the stationID and mean min and mean max of each year as a Text each.
		 */
		
		private Text result = new Text();
		private Text word = new Text();
		
		/*
		 * Every reduce call in secondary sort program will receive the largest stationID_year for a single stationID as the key and all 
		 * the years’ data sorted based on the year. For example, for staionID ABC, the key it would receive would be ABC_1889 and the 
		 * values it would receive would be [{1880, type1, temp1}, {1881, type2, temp2}, {1881, type3, temp3}...{1889, typen, tempn}]. 
		 * The keys that the reduce function would receive would be in sorted order based on staionID.
		 */
		
		public void reduce(Text key, Iterable<DataWritable> values, Context context) throws IOException, InterruptedException {
			
			LinkedHashMap<String, double[]> sS = new LinkedHashMap<String, double[]>();
			String station = key.toString().split("_")[0];
			
			for (DataWritable val : values) {
				if (sS.containsKey(val.getYear())) {
					double[] temper = sS.get(val.getYear());
					if (val.getType().equals("TMAX")) {
						temper[2] += val.getTemperature();
						temper[3] += 1;
					} else {
						temper[0] += val.getTemperature();
						temper[1] += 1;
					}
					
					sS.put(val.getYear(), temper);
				} else {
					
					double[] temper = new double[4];
					if (val.getType().equals("TMAX")) {
						temper[2] = val.getTemperature();
						temper[3] = 1;
					} else {
						temper[0] = val.getTemperature();
						temper[1] = 1;
					}
					
					sS.put(val.getYear(), temper);
				}
			}
			
			Iterator it = sS.entrySet().iterator();
			StringBuffer st = new StringBuffer();
			st.append("[");
			while (it.hasNext()) {
				@SuppressWarnings("unchecked")
				Map.Entry<String, double[]> pair = (Map.Entry<String, double[]>)it.next();
				st.append("(" + pair.getKey() + ", " + Math.round((pair.getValue()[0] / pair.getValue()[1]) * 100.0) / 100.0 + ", " + Math.round((pair.getValue()[2] / pair.getValue()[3]) * 100.0) / 100.0 + "),");
			}
			st.delete(st.length() - 1, st.length());
			st.append("]");
			result.set(st.toString());
			word.set(station);
			context.write(word, result);
		}
	}
	
	/*
	 * This is the main function for the MapReduce program. This is where all the mapper and reducer classes are assigned
	 * along with any partitioners, combiners and comparators, if any.
	 */

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "secondary sort");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(SecondarySortMapper.class);
		job.setMapOutputValueClass(DataWritable.class);
		job.setPartitionerClass(SecondarySortPartitioner.class);
		job.setSortComparatorClass(SecondarySortKeyComparator.class);
		job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
		job.setReducerClass(SecondarySortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
