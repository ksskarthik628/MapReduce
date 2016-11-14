package org.assignment1;

import java.util.*;

public class Sequential {
	
	/*
	 * This is the class that handles Sequential runs for the program. The constructor for the class
	 * takes as input the input data that is loaded into memory in the main function using a Loader class.
	 * The getAverage function takes as input a boolean value that indicated whether fibonacci should
	 * or should not run during the course of calculating the average TMAX for each station.
	 * The required data is stored in a HashMap<String, double]> data structure where the String 
	 * indicates the stationID which acts as the key and the value is a double[] array with two elements,
	 * the first indicating the average TMAX and the second indicating the number of records processed for that
	 * stationID.
	 */
	
	private List<String> inp; 
	private HashMap<String, double[]> records;
	private long start;
	private long end;
	
	Sequential(List<String> input) {
		inp = input;
		records = new HashMap<String, double[]>();
	}
	
	public long getAverage(boolean fib) {
		
		average(fib);
		
//		Uncomment for outputting the final data to STD out
		
//		System.out.print("\nSequential execution ");
//		if (fib)
//			System.out.print("with");
//		else
//			System.out.print("without");
//		System.out.print(" Fibonacci took " + (end - start) + " milliseconds\n");
//		records.forEach((key, value) -> {
//			System.out.println(key + ":" + value[0] + "," + value[1]);
//		});
		
		return (end - start);
	}
	
	private void average(boolean yesFib) {
		start = System.currentTimeMillis();
		/*
		 * For each line in the input data structure, if the entry has TMAX, then split
		 * the line based on ','. The first element in the split array indicates the stationID
		 * and the fourth element indicates the TMAX for the stationID. If fibonacci is to be run, then run
		 * it before putting the data into the records.
		 */
		for (String item : inp) {
			if (item.contains("TMAX")) {
				String[] record = item.split(",");
				if (records.containsKey(record[0])) { // if current stationID is in records
					double[] temp = records.get(record[0]);
					if (yesFib)
						fib(17);
					temp[0] = ((temp[0] * temp[1]) + Double.parseDouble(record[3])) / (temp[1] + 1);
					temp[1] += 1;
					records.put(record[0], temp);
				} else { // if current stationID is not in records
					double[] temp = new double[2];
					if (yesFib)
						fib(17);
					temp[0] = Double.parseDouble(record[3]);
					temp[1] = 1;
					records.put(record[0], temp);
				}
			}
		}
		end = System.currentTimeMillis();
	}
	
	/*
	 * Standard fibonacci function 
	 */
	private int fib(int n) {
		if (n == 0 || n == 1)
			return 1;
		else
			return fib(n - 1) + fib(n - 2);
	}

}
