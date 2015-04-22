package ts.hadoop.pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends
		Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//Assign attributes to page from line
		Page page = new Page(value.toString());
				
		//Emit output: key = linkN  value = PageN:pageRank:votes
		for(String link : page.getLinks()){
			StringBuilder sb = new StringBuilder();
			sb.append(page.getPageId() + ":" + page.getRank() + ":" + page.getLinks().size());
			context.write(new Text(link), new Text(sb.toString()));			
			System.out.println(link + "\t" + sb.toString());
		}
		
		//Emit output: PageN, RankN, and links
		String links = String.join(",", page.getLinks());
		context.write(new Text(page.getPageId()), new Text(links.toString()));
		System.out.println(page.getPageId()+"\t"+links.toString());
	}

}
