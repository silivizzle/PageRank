package ts.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainDriver {
	
	public static int pageCount = 0;
	private static String input, output;
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		boolean run = true;
		int iterationCount = 0;
		List<Double> previousRank = new ArrayList<Double>();
		List<Double> currentRank = new ArrayList<Double>();
		double sigma = 0.05;
		double rank;
		double change;
		FileSystem fs = FileSystem.get(new Configuration());
		Path inFile = new Path(args[0] + "/test.txt");
		
		//Get number of pages and initial ranks
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)));
		while(br.readLine() != null){
			//line: pageId|rank<TAB>link1,link2,...,linkN
			String[] line = br.readLine().split("\\|");
			String[] values = line[1].split("\t");
			rank = Double.parseDouble(values[0].toString());
			previousRank.add(rank);
			pageCount++;
		}
		br.close();
		
		//Initialize currentRanks list to 0s
		for(int i=0; i<pageCount; i++){
			currentRank.add((double) 1/pageCount);
		}
		
		while(run){
	
			//Job
			Job job = new Job();
			job.setJarByClass(MainDriver.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setJobName("PageRank Example");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			if(iterationCount == 0){
				//Set paths
				input = args[0];				
			}
			else 
				input = args[1] + "_" + iterationCount;
				output = args[1] + "_" + (iterationCount + 1);
						 
			FileInputFormat.setInputPaths(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
	
			job.waitForCompletion(true);
			change = compareSigma(previousRank, currentRank);
			if(change < sigma){
				run = false;
			}
			else
				previousRank = getRank(new Path(input));
				currentRank = getRank(new Path(output));
				iterationCount++;		
		}
		
	};
	
	public static double compareSigma(List<Double> prevRank, List<Double> curRank){
		double change = 0;
		double sum = 0;
		for(int i=0; i<pageCount; i++){
			sum += Math.pow((curRank.get(i) - prevRank.get(i)), 2);
			change = Math.sqrt(sum);
		}
		return change;
		
	}
	
	public static List<Double> getRank(Path path) throws IOException{
		List<Double> rankList = new ArrayList<Double>();
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		double rank = 0;
		while(br.readLine() != null){
			String[] line = br.readLine().split("|");
			String[] values = line[1].split("\t");
			rank = Double.parseDouble(values[0].toString());
			rankList.add(rank);
		}
		br.close();
		return rankList;		
	}

}
