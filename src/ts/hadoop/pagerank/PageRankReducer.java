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
			double newRank = 0.0, sum = 0.0, rankLink = 0.0, numOutLinks = 0.0, beta = 0.85;
			String links = "";
			page.setPageId(key.toString());
			
			for(Text value : values){
				
				//Capture if line looks like "#link1,...,linkN"
				if( value.toString().startsWith("#")){
					links = value.toString().replaceFirst("#", "");
				}
				else{
				//process if line looks like "inLink:rank:numLinks
					String[] line = value.toString().split(":");
				//set variables					
					rankLink = Double.parseDouble(line[1]);
					numOutLinks = Double.parseDouble(line[2]);
					sum += (rankLink/numOutLinks);
				}
			}//end for loop			
				newRank = ((1-beta)/MainDriver.PAGE_COUNT) + beta*(sum);
				StringBuilder sb = new StringBuilder();
				sb.append(Double.toString(newRank) + "\t" + links);
				Text newInfo = new Text(sb.toString());
				context.write(key, newInfo);
				System.out.println(key + newInfo.toString());
		}//end while loop
		
	}//end void
			
}
