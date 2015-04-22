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
			double rankLink, linkVotes;
			String [] pageValues;
			double n = MainDriver.pageCount;
			page.setPageId(key.toString());
			
			for(Text value : values){
				
				//if line looks like "pageId:rank/votes"
				if(value.toString().contains(":")){
				
					//separate pageId from rank/votes
					String[] line = value.toString().split(":");
					
					//single out the rank of the in link and its votes
					if(line.toString().contains("/")){						
						pageValues = line[1].split("/");
						rankLink = Double.parseDouble(pageValues[0]);
						linkVotes = Double.parseDouble(pageValues[1]);
					}
					else{
						rankLink = Double.parseDouble(line[1]);
						linkVotes = 1;
					}	
					
					newRank += (beta * (rankLink/linkVotes)) + (1-beta)/n;
				}
				//Emit new rank
				else{
					
					//combine pageId with its new rank and place the outlinks as the output
					Text newKey = new Text(page.getPageId().toString() + "|" + Double.toString(newRank) + "\t");
					context.write(newKey, new Text(values.toString()));
				}
			
				
				
			}
			
		}
	}

}
