package assignment3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

@SuppressWarnings("unused")
public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	
	/*
	 * This is the reducer class for calculating page ranks for all the pages under consideration. The reduce
	 * function takes as key the page name for which page rank calculation is being done, and a list of Text objects
	 * which contain various things pertaining to the current page. It also gets the count of the total number of
	 * pages in the input files combined (COUNT) which it uses to update the page rank.
	 */

	// randomness factor for all page rank calculations.
	private static final double randomness = 0.85;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		/*
		 * The reduce function gets as key a Text object with page name and a list of Text objects which can contain 
		 * several things:
		 * - the Text could start with the symbol "!" indicating that the current page is a real page and not a ghost
		 *   page, in which case a boolean is set to indicate it is a real page
		 * - the Text could start with the symbol "|" indicating that everything that comes after this symbol is the
		 *   adjacency list for the current page in consideration
		 * - the Text could contain the page rank of the page to which it was adjacent and that main page's number of
		 *   adjacent elements, required for updating the page rank of current page in consideration
		 * The function also gets the count of the total number of pages from the Context object, which is useful for
		 * updating the page rank. After the page rank has been updated, and if the page is a real page instead of a ghost
		 * page, the function outputs the page name as key and updated page rank with adjacency list as values.
		 */
	
		// initialization
		boolean realPage = false;
        String[] split;
        float sumPageRank = 0;
        String links = "";
        String rankWithLinks;
        
 
        for(Text value : values){
        	rankWithLinks = value.toString();
        	
        	// if the page is a real page, save that information
            if(rankWithLinks.equals("!")) {
                realPage = true;
                continue;
            }
 
            // store the adjacency list for the current page
            if(rankWithLinks.startsWith("|")){
                links = "\t" + rankWithLinks.substring(1);
                continue;
            }
 
            // get the page rank contribution from adjacent pages and store their sum
            split = rankWithLinks.split("\t");
            float pageRank = Float.parseFloat(split[0]);
            int countOutLinks = Integer.parseInt(split[1]);
 
            sumPageRank += (pageRank / countOutLinks);
        }
 
        // if the page is a ghost, don't output anything
        if(!realPage) return;
        
        // if the page is real, get the total number of pages and update page rank
        double count = Double.parseDouble(context.getConfiguration().get("COUNT"));
        double newRank = randomness * sumPageRank + ((1.0 - randomness) / count);
 
        // output page as key and page rank + adjacency list as value
        context.write(key, new Text(newRank + links));
		
	}
	
}