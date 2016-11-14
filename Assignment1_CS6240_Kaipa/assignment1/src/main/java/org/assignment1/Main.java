package org.assignment1;

import java.util.*;

public class Main {

	/*
	 * This is the main class for Part 1 of Assignment 1. The program takes in one input
	 * which is the location of the data file to be used. The data file is read one line at a time and
	 * the data so collected is stored in a List<String> data structure. Once the loading is done,
	 * the program executes the TMAX average function for all station IDs in the order Sequential,
	 * Threaded No Lock, Threaded Coarse Lock, Threaded Fine Lock and Threaded No Sharing.
	 * The number of times each version of the code runs is controlled by a variable declared in the main
	 * function of the program. For each run of each version, an execution without fibonacci and an execution with
	 * fibonacci are run and their run times (returned by the function) are recorded for calculation of
	 * average, min and max execution times. For the purpose of debugging and data collection, the
	 * functions are currently not outputting any of the computed values to STD out.
	 */
	
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Please use proper syntax. Refer the readme. Terminating.");
			return;
		}
		
		// Controls the number of runs for each version of the code
		int runTimes = 10;
		
		// Load the data from the file into memory using a List<String>
		String inputFilename = args[0];
		System.out.print("Loading data into memory...");
		Loader load = new Loader(inputFilename);
		List<String> input = load.loadIntoMemory();
		System.out.println("Done!\n\n");
		
		System.out.println("Executing Sequential, No Lock, Coarse Lock, Fine Lock and No Sharing sequentially\n\n");
		
		// Initialize the data structures for running and recording the times for Sequential version
		System.out.print("Initializing run for Sequential...");
		Sequential[] seq = new Sequential[runTimes];
		long[] seqNoF = new long[runTimes]; // run times for no fibonacci 
		long[] seqF = new long[runTimes]; // run times for fibonacci
		for (int i = 0; i < runTimes; i++) {
			seq[i] = new Sequential(input); // initialize for sequential run
		}
		System.out.print("Done\n");
		System.out.println("Running Sequential " + runTimes + " times...");
		for (int i = 0; i < runTimes; i++) {
			seqNoF[i] = seq[i].getAverage(false); // run without fibonacci
			seqF[i] = seq[i].getAverage(true); // run with fibonacci
		}
		System.out.println("Done\n");
		
		// Initialize the data structures for running and recording the times for Threaded No Lock version
		System.out.print("Initializing run for No Lock...");
		ThreadNoLock[] tnl = new ThreadNoLock[runTimes];
		long[] tnlNoF = new long[runTimes]; // run times for no fibonacci 
		long[] tnlF = new long[runTimes]; // run times for fibonacci
		for (int i = 0; i < runTimes; i++) {
			tnl[i] = new ThreadNoLock(input); // initialize for no lock run
		}
		System.out.print("Done\n");
		System.out.println("Running No Lock " + runTimes + " times...");
		for (int i = 0; i < runTimes; i++) {
			tnlNoF[i] = tnl[i].getAverage(false); // run without fibonacci
			tnlF[i] = tnl[i].getAverage(true); // run with fibonacci
		}
		System.out.println("Done\n");
		
		// Initialize the data structures for running and recording the times for Treaded Coarse Lock version
		System.out.print("Initializing run for Coarse Lock...");
		ThreadCrudeLock[] tcl = new ThreadCrudeLock[runTimes];
		long[] tclNoF = new long[runTimes]; // run times for no fibonacci
		long[] tclF = new long[runTimes]; // run times for fibonacci
		for (int i = 0; i < runTimes; i++) {
			tcl[i] = new ThreadCrudeLock(input); // initialize for coarse lock run
		}
		System.out.print("Done\n");
		System.out.println("Running Coarse Lock " + runTimes + " times...");
		for (int i = 0; i < runTimes; i++) {
			tclNoF[i] = tcl[i].getAverage(false); // run without fibonacci
			tclF[i] = tcl[i].getAverage(true); // run with fibonacci
		}
		System.out.println("Done\n");
		
		// Initialize the data structures for running and recording the times for Treaded Fine Lock version
		System.out.print("Initializing run for Fine Lock...");
		ThreadFineLock[] tfl = new ThreadFineLock[runTimes];
		long[] tflNoF = new long[runTimes]; // run times for no fibonacci
		long[] tflF = new long[runTimes]; // run times for fibonacci
		for (int i = 0; i < runTimes; i++) {
			tfl[i] = new ThreadFineLock(input); // initialize for fine lock run
		}
		System.out.print("Done\n");
		System.out.println("Running Fine Lock " + runTimes + " times...");
		for (int i = 0; i < runTimes; i++) {
			tflNoF[i] = tfl[i].getAverage(false); // run without fibonacci
			tflF[i] = tfl[i].getAverage(true); // run with fibonacci
		}
		System.out.println("Done\n");
		
		// Initialize the data structures for running and recording the times for Treaded No Sharing verison
		System.out.print("Initializing run for No Sharing...");
		NoSharing[] ns = new NoSharing[runTimes];
		long[] nsNoF = new long[runTimes]; // run times without fibonacci
		long[] nsF = new long[runTimes]; // run times with fibonacci
		for (int i = 0; i < runTimes; i++) {
			ns[i] = new NoSharing(input); // initialize for no sharing run
		}
		System.out.print("Done\n");
		System.out.println("Running No Sharing " + runTimes + " times...");
		for (int i = 0; i < runTimes; i++) {
			nsNoF[i] = ns[i].getAverage(false); // run without fibonacci
			nsF[i] = ns[i].getAverage(true); // run with fibonacci
		}
		System.out.println("Done\n");
		
		// Initialize variables for storing the maximum, minimum, and average run times for each version with and without fibonacci
		long seqMaxNF = Long.MIN_VALUE;
		long seqMinNF = Long.MAX_VALUE;
		long seqAvgNF = 0;
		long seqMaxF = Long.MIN_VALUE;
		long seqMinF = Long.MAX_VALUE;
		long seqAvgF = 0;
		
		long tnlMaxNF = Long.MIN_VALUE;
		long tnlMinNF = Long.MAX_VALUE;
		long tnlAvgNF = 0;
		long tnlMaxF = Long.MIN_VALUE;
		long tnlMinF = Long.MAX_VALUE;
		long tnlAvgF = 0;
		
		long tclMaxNF = Long.MIN_VALUE;
		long tclMinNF = Long.MAX_VALUE;
		long tclAvgNF = 0;
		long tclMaxF = Long.MIN_VALUE;
		long tclMinF = Long.MAX_VALUE;
		long tclAvgF = 0;
		
		long tflMaxNF = Long.MIN_VALUE;
		long tflMinNF = Long.MAX_VALUE;
		long tflAvgNF = 0;
		long tflMaxF = Long.MIN_VALUE;
		long tflMinF = Long.MAX_VALUE;
		long tflAvgF = 0;
		
		long nsMaxNF = Long.MIN_VALUE;
		long nsMinNF = Long.MAX_VALUE;
		long nsAvgNF = 0;
		long nsMaxF = Long.MIN_VALUE;
		long nsMinF = Long.MAX_VALUE;
		long nsAvgF = 0;
		
		// For each run of each version of each flavor, calculate the max, min and avg run times
		for (int i = 0; i < runTimes; i++) {
			if (seqMaxNF < seqNoF[i])
				seqMaxNF = seqNoF[i];
			if (seqMaxF < seqF[i])
				seqMaxF = seqF[i];
			if (seqMinNF > seqNoF[i])
				seqMinNF = seqNoF[i];
			if (seqMinF > seqF[i])
				seqMinF = seqF[i];
			
			if (tnlMaxNF < tnlNoF[i])
				tnlMaxNF = tnlNoF[i];
			if (tnlMaxF < tnlF[i])
				tnlMaxF = tnlF[i];
			if (tnlMinNF > tnlNoF[i])
				tnlMinNF = tnlNoF[i];
			if (tnlMinF > tnlF[i])
				tnlMinF = tnlF[i];
			
			if (tclMaxNF < tclNoF[i])
				tclMaxNF = tclNoF[i];
			if (tclMaxF < tclF[i])
				tclMaxF = tclF[i];
			if (tclMinNF > tclNoF[i])
				tclMinNF = tclNoF[i];
			if (tclMinF > tclF[i])
				tclMinF = tclF[i];
			
			if (tflMaxNF < tflNoF[i])
				tflMaxNF = tflNoF[i];
			if (tflMaxF < tflF[i])
				tflMaxF = tflF[i];
			if (tflMinNF > tflNoF[i])
				tflMinNF = tflNoF[i];
			if (tflMinF > tflF[i])
				tflMinF = tflF[i];
			
			if (nsMaxNF < nsNoF[i])
				nsMaxNF = nsNoF[i];
			if (nsMaxF < nsF[i])
				nsMaxF = nsF[i];
			if (nsMinNF > nsNoF[i])
				nsMinNF = nsNoF[i];
			if (nsMinF > nsF[i])
				nsMinF = nsF[i];
			
			seqAvgNF += seqNoF[i];
			seqAvgF += seqF[i];
			
			tnlAvgNF += tnlNoF[i];
			tnlAvgF += tnlF[i];
			
			tclAvgNF += tclNoF[i];
			tclAvgF += tclF[i];
			
			tflAvgNF += tflNoF[i];
			tflAvgF += tflF[i];
			
			nsAvgNF += nsNoF[i];
			nsAvgF += nsF[i];
		}
		
		seqAvgNF /= runTimes;
		seqAvgF /= runTimes;
		
		tnlAvgNF /= runTimes;
		tnlAvgF /= runTimes;
		
		tclAvgNF /= runTimes;
		tclAvgF /= runTimes;
		
		tflAvgNF /= runTimes;
		tflAvgF /= runTimes;
		
		nsAvgNF /= runTimes;
		nsAvgF /= runTimes;
		
		// Output the data relevant to the runs
		System.out.println("\nFor Sequential run:");
		System.out.println("Without Fibonacci:");
		System.out.println("Max:" + seqMaxNF + " Min:" + seqMinNF + " Avg:" + seqAvgNF);
		System.out.println("With Fibonacci:");
		System.out.println("Max:" + seqMaxF + " Min:" + seqMinF + " Avg:" + seqAvgF);
		
		System.out.println("\nFor No Lock run:");
		System.out.println("Without Fibonacci:");
		System.out.println("Max:" + tnlMaxNF + " Min:" + tnlMinNF + " Avg:" + tnlAvgNF);
		System.out.println("With Fibonacci:");
		System.out.println("Max:" + tnlMaxF + " Min:" + tnlMinF + " Avg:" + tnlAvgF);
		
		System.out.println("\nFor Coarse Lock run:");
		System.out.println("Without Fibonacci:");
		System.out.println("Max:" + tclMaxNF + " Min:" + tclMinNF + " Avg:" + tclAvgNF);
		System.out.println("With Fibonacci:");
		System.out.println("Max:" + tclMaxF + " Min:" + tclMinF + " Avg:" + tclAvgF);
		
		System.out.println("\nFor Fine Lock run:");
		System.out.println("Without Fibonacci:");
		System.out.println("Max:" + tflMaxNF + " Min:" + tflMinNF + " Avg:" + tflAvgNF);
		System.out.println("With Fibonacci:");
		System.out.println("Max:" + tflMaxF + " Min:" + tflMinF + " Avg:" + tflAvgF);
		
		System.out.println("\nFor No Sharing run:");
		System.out.println("Without Fibonacci:");
		System.out.println("Max:" + nsMaxNF + " Min:" + nsMinNF + " Avg:" + nsAvgNF);
		System.out.println("With Fibonacci:");
		System.out.println("Max:" + nsMaxF + " Min:" + nsMinF + " Avg:" + nsAvgF);
		
	}

}
