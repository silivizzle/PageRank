package ts.hadoop.pagerank;

import java.util.ArrayList;
import java.util.List;

public class Page {
	
	private String pageId, rank;
	private List<String> links = new ArrayList<String>();
	
	public Page(){
		
	}
	
	public Page(String pageId, String rank, List<String> links){
		this.pageId = pageId;
		this.rank = rank;
		this.links = links;
	}	
	//Assign key value pair
	public Page(String pageInfo){
		//input: pageId<TAB>pageRank<TAB>Link1,Link2,...,LinkN
		String[] input = pageInfo.split("\t");
		//Assign node attributes
		this.pageId = input[0];	
		this.rank = input[1];
		for(String link : input[2].split(",")){
			this.links.add(link);
		}
	}

	public String getPageId() {
		return pageId;
	}

	public void setPageId(String pageId) {
		this.pageId = pageId;
	}

	public String getRank() {
		return rank;
	}

	public void setRank(String rank) {
		this.rank = rank;
	}

	public List<String> getLinks() {
		return links;
	}

	public void setLinks(List<String> links) {
		this.links = links;
	}
	
	public String getNumLinks(){
		String numLinks = Integer.toString(links.size());
		return numLinks;
		
	}
	
}
