package ts.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainDriver {
	
	public static double PAGE_COUNT = 100;
	private static String input, output;
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		int iterationCount = 0;
		
		while(iterationCount < 3){
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
			fs.delete(new Path(output), true);
			FileInputFormat.setInputPaths(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			iterationCount++;
			
		}
		
	};

}
