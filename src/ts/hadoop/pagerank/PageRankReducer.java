package ts.hadoop.pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		while (values.iterator().hasNext()) {
			
			Page page = new Page();
			double newRank = 0.0;
			double beta = 0.85;
			double rankLink = 0, numOutLinks = 0;
			double n = MainDriver.pageCount;
			String links = "";
			page.setPageId(key.toString());
			for(Text value : values){
				
				//Capture if line looks like "link1,...,linkN"
				if( value.toString().contains(",")){
					System.out.println(key + "\t" + value + " contains ','");
					links = value.toString();
				}
				else
				//process if line looks like "inLink:rank:numLinks
					System.out.println(key + "\t" + value + " contains ':'");
					String[] line = value.toString().split(":");
					System.out.println("Line array length: " + line.length);
				//set variables
				if(line.length == 3){						
					rankLink = Double.parseDouble(line[1]);
					numOutLinks = Double.parseDouble(line[2]);
				}
				else if(line.length == 1){
					System.out.println("Line array length: " + line.length);
					rankLink = Double.parseDouble(line[1]);
					numOutLinks = 1;
				}
				
				newRank += beta*(rankLink/numOutLinks) + ((1-beta)/n);
		
				System.out.println(key + "\t " + "Links: " + links);
					
					
					//combine pageId with its new rank and place the outlinks as the output
					Text newKey = new Text(page.getPageId().toString() + "|" + Double.toString(newRank) + "\t");
					context.write(newKey, new Text(links));
			}//end for loop
		
		}//end while loop
			
	}
}
