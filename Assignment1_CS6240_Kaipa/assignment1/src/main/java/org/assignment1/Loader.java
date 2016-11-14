package org.assignment1;

import java.util.*;
import java.io.*;

public class Loader {
	
	/*
	 * This is the loader class for the program which is responsible for reading the
	 * input file and returning each line in the file as an entry in a List<String>
	 */
	
	private String flname;
	Loader(String filename) {
		flname = filename; 
	}
	
	public List<String> loadIntoMemory() {
		List<String> data = new ArrayList<String>();
		String line = null;
		
		try {
			FileReader fileReader = new FileReader(flname);
			BufferedReader bufferReader = new BufferedReader(fileReader);
			
			while((line = bufferReader.readLine()) != null) {
				data.add(line);
			}
			
			bufferReader.close();
			
		} catch(Exception e) {
			System.out.println(e.getMessage());
		}
		
		return data;
	}
}
