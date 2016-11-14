package assignment3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class PageRankMapper extends Mapper<Object, Text, Text, Text> {
	
	/*
	 * This is the mapper class for calculating page ranks for all the pages under consideration. The map
	 * function takes as input the page name, page rank so far, adjacency list for the page, and COUNT and
	 * DELTA counters from the previous MapReduce job to update the page ranks of each page with the contribution
	 * from dangling nodes that got missed in the previous job. The mapper also updates the number of pages it sees
	 * in it's own counter COUNT which will later be used in the next job to update the page rank with contributions
	 * from dangling nodes.
	 */
	
	// randomness factor for all page rank calculations.
	private static final double randomness = 0.85;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		/*
		 * The map function gets as input a Text object which contains the page name, it's previous page rank
		 * and it's adjacency list. The function parses these out and stores them for use. It also receives the 
		 * DELTA and COUNT counters from the previous job run and uses these values to update the page rank with
		 * dangling node contributions. For every page in the current page's adjacency list, the map function
		 * outputs the page name, the rank of the current page and the number of elements in it's adjacency list.
		 * It also outputs an entry with current page name and a Text object containing "!" to indicate that the
		 * page is a real page and not a ghost link to avoid bogus pages for subsequent jobs. It also outputs the
		 * current page along with it's adjacency list to be used by the reducer to store in it's output for use in
		 * subsequent jobs. 
		 */
		
		int pageIndex = value.find("\t");
        int rankIndex = value.find("\t", pageIndex + 1);
 
        // get page name and rank
        String page = Text.decode(value.getBytes(), 0, pageIndex);
        String rank = value.toString().split("\t")[1];
        		
        // update rank with dangling node contribution
        double previousDelta = Double.parseDouble(context.getConfiguration().get("DELTA")) / Math.pow(10.0, 12);
        double count = Double.parseDouble(context.getConfiguration().get("COUNT"));
        previousDelta /= count;
        
        if (rank.equals("NaN"))
        	rank = Double.toString(1.0 / count);
        
        double correctedRank = Double.parseDouble(rank) + (randomness * previousDelta);
        
        rank = Double.toString(correctedRank);
        
        // output to reducer saying the current page is a real page
        context.write(new Text(page), new Text("!"));
        
        // if the page has no adjacent elements (dangling node), update DELTA counter
        if (rankIndex == -1) {
        	context.getCounter(DANGLING_COUNTER.DELTA).increment((long) (correctedRank * Math.pow(10.0, 12)));
        	return;
        }
        
        // get the adjacent pages and the number of adjacent pages
        String links = Text.decode(value.getBytes(), rankIndex + 1, value.getLength() - (rankIndex + 1));
        String[] edges = links.split("\t");
        int totalEdges = edges.length;
 
        // for every adjacent page, output the page name, current page's rank and the number of adjacent pages
        for (String link : edges){
            Text pageRankTotalLinks = new Text(rank + "\t" +  totalEdges);
            context.write(new Text(link), pageRankTotalLinks);
        }
 
        // update the COUNT counter for every new page and output page wit adjacency list
        context.getCounter(DANGLING_COUNTER.COUNT).increment(1);
        context.write(new Text(page), new Text("|" + links));
		
	}
	
}