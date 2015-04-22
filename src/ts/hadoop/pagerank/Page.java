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
		//input: pageId|pageRank<TAB>Link1,Link2,...,LinkN
		String[] input = pageInfo.split("\\|");
		String key = "", value = "";	
		try{
			key = input[0];
			value = input[1];
		}
		catch(Exception e){
			e.printStackTrace();
			System.exit(1);
		}
		
		//Assign node attributes
		this.pageId = key;
		String[] tokens = value.split("\t");		
		this.rank = tokens[0];
		for(String link : tokens[1].split(",")){
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
	
	public void printPageInfo(){
		System.out.println("================");
		System.out.println("Page Id: " + pageId);
		System.out.println("Rank: " + rank);
		System.out.println("Links: " + getNumLinks());
		System.out.println("================");
	}
	
	

}
