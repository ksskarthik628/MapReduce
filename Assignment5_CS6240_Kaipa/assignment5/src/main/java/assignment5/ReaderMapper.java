package assignment5;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

@SuppressWarnings("unused")
public class ReaderMapper extends Mapper<Object, Text, Text, Text> {
	
	/*
	 * This is the mapper class for parsing the data contained in bz2 files, accessing the html pages in
	 * each line and parsing out the adjacency list based on the sample parser program provided by the
	 * professor. Since this job is a map only job, the adjacency list is formed in the map function
	 * itself. The mapper also updates the number of pages it sees in the counter COUNT for use in the next job,
	 * the first Page Rank job. The counter DELTA remains 0 so it will not have any effect on the page
	 * rank calculation in the next job.
	 */

	private static Pattern namePattern;
	private static Pattern linkPattern;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		try {

			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			XMLReader xmlReader = saxParser.getXMLReader();
			// Parser fills this list with linked page names.
			List<String> linkPageNames = new LinkedList<String>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));
			
			String line = value.toString();
			
			// Each line formatted as (Wiki-page-name:Wiki-page-html).
			int delimLoc = line.indexOf(':');
			String pageName = line.substring(0, delimLoc);
			String html = line.substring(delimLoc + 1);
			Matcher matcher = namePattern.matcher(pageName);
			if (!matcher.find()) {
				// Skip this html file, name contains (~).
				return;
			}

			// Parse page and fill list of linked pages.
			linkPageNames.clear();
			try {
				xmlReader.parse(new InputSource(new StringReader(html)));
			} catch (Exception e) {
				// Discard ill-formatted pages.
				return;
			}
			
			// create StringBuilder for adjacency list and store initial page rank value of NaN
			StringBuilder res = new StringBuilder();

			// add all adjacent nodes if any to StringBuilder
			for (String page : linkPageNames) {
				res.append("\t");
				res.append(page);
			}
			
			// write to context the page name and it's adjacency list with initial page rank
			// increment count of number of valid nodes
			context.getCounter(COUNTER.COUNT).increment(1);
			context.write(new Text(pageName), new Text(res.toString()));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/* Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {
		/* List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/* Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}
	}
	
}
