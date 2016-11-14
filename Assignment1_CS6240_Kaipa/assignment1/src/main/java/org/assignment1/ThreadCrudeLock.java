package org.assignment1;

import java.util.*;

public class ThreadCrudeLock {
	
	/*
	 * This is the class that handles Threaded Coarse Lock runs for the program. The constructor for the class
	 * takes as input the input data that is loaded into memory in the main function using a Loader class.
	 * The getAverage function takes as input a boolean value that indicated whether fibonacci should
	 * or should not run during the course of calculating the average TMAX for each station.
	 * The required data is stored in a HashMap<String, double]> data structure where the String 
	 * indicates the stationID which acts as the key and the value is a double[] array with two elements,
	 * the first indicating the average TMAX and the second indicating the number of records processed for that
	 * stationID. The getAverage function creates the number of threads equivalent to the number of logical 
	 * cores that the CPU has. There is a locking mechanism in place for updating and inserting data into
	 * the records data structure. Every thread, when inserting or updating data into the records data structure
	 * applies a lock on the entire data structure, disabling any other thread from performing the same 
	 * functions simultaneously. This does not cause null pointer exceptions but does cause the execution to be slow
	 */

	private List<String> inp;
	private Map<String, double[]> records;
	private long start;
	private long end;
	
	ThreadCrudeLock(List<String> input) {
		inp = input;
		records = Collections.synchronizedMap(new HashMap<String, double[]>());
	}
	
	/*
	 * Create as many threads as there are logical cores in the CPU and run them one after the other.
	 * Join the threads to the main thread so that it waits for the execution of all threads to complete
	 * before continuing with returning the run time.
	 */
	public long getAverage(boolean fib) {
		int cores = Runtime.getRuntime().availableProcessors();
		CrudeLock[] crudeLocks = new CrudeLock[cores];
		Thread[] threads = new Thread[cores];
		int size = inp.size();
		for (int i = 0; i < cores; i++) {
			crudeLocks[i] = new CrudeLock(inp.subList((size * i) / cores, (size * (i + 1)) / cores), fib);
		}
		for (int i = 0; i < cores; i++) {
			threads[i] = new Thread(crudeLocks[i]);
		}
		for (int i = 0; i < cores; i++) {
			if (i == 0)
				start = System.currentTimeMillis();
			threads[i].start();
		}
		for (int i = 0; i < cores; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}
		}
		end = System.currentTimeMillis();

//		Uncomment for outputting the final data to STD out
		
//		System.out.print("\nCoarse Lock execution ");
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
	
	/*
	 * Implementing the Runnable interface instead of extending the Thread class is better practice.
	 * The main function that executes is the average function and it runs exactly the same as Sequential
	 * run but only works on a part of the input record. It puts locks on the entire data structure
	 * and because of this, the output data is inconsistent but execution time takes a hit  
	 */
	
	class CrudeLock implements Runnable {
		
		private List<String> inp;
		private boolean yesFib;
		
		CrudeLock(List<String> input, boolean fib) {
			inp = input;
			yesFib = fib;
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
		
		private void average() {
			for (String item : inp) {
				if (item.contains("TMAX")) {
					String[] record = item.split(",");
					synchronized(records) { // apply lock on the whole of records
						if (records.containsKey(record[0])) { // if the current stationID is in records
							double[] temp = records.get(record[0]);
							if (yesFib)
								fib(17);
							temp[0] = ((temp[0] * temp[1]) + Double.parseDouble(record[3])) / (temp[1] + 1);
							temp[1] += 1;
							records.put(record[0], temp);
						} else { // if te current stationID is not in records
							double[] temp = new double[2];
							if (yesFib)
								fib(17);
							temp[0] = Double.parseDouble(record[3]);
							temp[1] = 1;
							records.put(record[0], temp);
						}
					}
				}
			}
		}

		
		public void run() {
			average();
		}
		
	}
	
}
