import java.io.*;
import java.lang.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CitationGraph extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res); 
	}
	
	public int run ( String[] args ) throws Exception {
		String input = "/class/s17419/lab3/patents.txt";
		String temp = "/scr/rvshah/lab3/exp1/temp";
		String output = "/scr/rvshah/lab3/exp1/output/";
		
		int reduce_tasks = 1;
		Configuration conf = new Configuration();
		
		Job job_one = new Job(conf, "Citation Graph Round One"); 
		
		job_one.setJarByClass(CitationGraph.class); 
		
		job_one.setNumReduceTasks(reduce_tasks);
		
		job_one.setOutputKeyClass(Text.class); 
		
		job_one.setOutputValueClass(IntWritable.class);
		
		job_one.setMapperClass(Map_One.class); 
		
		job_one.setReducerClass(Reduce_One.class);
		
		job_one.setInputFormatClass(TextInputFormat.class);  
		
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job_one, new Path(input)); 

		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		
		// Run the job
		job_one.waitForCompletion(true); 
		return 0;
	} // End run
	
	// The Map Class
	public static class Map_One extends Mapper<LongWritable, Text, Text, IntWritable>  {
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException  {
		} // End method "map"
	} // End Class Map_One
	
	// The reduce class
	public static class Reduce_One extends Reducer<Text, IntWritable, Text, IntWritable>  {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException  {
		} // End method "reduce" 
	} // End Class Reduce_One
	
	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, IntWritable>  {
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException  {
		}  // End method "map"
	}  // End Class Map_Two
	
	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, IntWritable, Text, IntWritable>  {
		IntWritable maxInt = new IntWritable();
		public void reduce(Text key, IntWritable value, Context context) 
				throws IOException, InterruptedException  {
		}  // End method "reduce"
	}  // End Class Reduce_Two
	
}