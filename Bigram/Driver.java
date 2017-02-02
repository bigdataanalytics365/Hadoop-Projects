/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 2 *********************
  * For question regarding this code,
  * please ask on Piazza
  *****************************************
  *****************************************
  */

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

public class Driver extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "/class/s17419/lab2/shakespeare";    // Change this accordingly
		String temp = "/scr/rvshah/lab2/exp2/temp";      // Change this accordingly
		String output = "/scr/rvshah/lab2/exp2/output/";  // Change this accordingly
		
		int reduce_tasks = 2;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();
		
		// Create job for round 1
		
		// Create the job
		Job job_one = new Job(conf, "Driver Program Round One"); 
		
		// Attach the job to this Driver
		job_one.setJarByClass(Driver.class); 
		
		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);
		
		// The datatype of the Output Key 
		// Must match with the declaration of the Reducer Class
		job_one.setOutputKeyClass(Text.class); 
		
		// The datatype of the Output Value 
		// Must match with the declaration of the Reducer Class
		job_one.setOutputValueClass(IntWritable.class);
		
		// The class that provides the map method
		job_one.setMapperClass(Map_One.class); 
		
		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);
		
		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);  
		
		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed
		
		// Run the job
		job_one.waitForCompletion(true); 
		
		
		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		
		Job job_two = new Job(conf, "Driver Program Round Two"); 
		job_two.setJarByClass(Driver.class); 
		job_two.setNumReduceTasks(reduce_tasks); 
		
		job_two.setOutputKeyClass(Text.class); 
		job_two.setOutputValueClass(IntWritable.class);
		
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class); 
		job_two.setReducerClass(Reduce_Two.class);
		
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp)); 
		FileOutputFormat.setOutputPath(job_two, new Path(output));
		
		// Run the job
		job_two.waitForCompletion(true); 
		
		/**
		 * **************************************
		 * **************************************
		 * FILL IN CODE FOR MORE JOBS IF YOU NEED
		 * **************************************
		 * **************************************
		 */
		
		return 0;
		
	} // End run
	
	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and IntWritable as value
	public static class Map_One extends Mapper<LongWritable, Text, Text, IntWritable>  {
				
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();
			// Replace all special characters with emapty string.
			line = line.replaceAll("\\p{Punct}+", "").toLowerCase();
			// Update the string to be all in lower case.
			line = line.toLowerCase();
			// Tokenize to get the individual words
			StringTokenizer tokens = new StringTokenizer(line);
			
			IntWritable one = new IntWritable(1);
			Text firstWord = new Text();
			Text secondWord = new Text();
			boolean first  = true;
			
			while (tokens.hasMoreTokens()) {
				/**
				 * ***********************************
				 * ***********************************
				 * FILL IN CODE FOR THE MAP FUNCTION
				 * ***********************************
				 * ***********************************
				 */
				// Check if secondWord has been set. This is to distinguish between first iteration from other.
				if(!secondWord.toString().equals("")){
					// Second word has been set. Update the first word with second word.
					firstWord.set(secondWord.toString());
					secondWord.set(tokens.nextToken());
				}else{
					// This is the first iteration. Check for token again when pulling second word.
					firstWord.set(tokens.nextToken());
					if(tokens.hasMoreTokens()){
						secondWord.set(tokens.nextToken());
					}
				} // End if - else the secondWord had been set.

				// If first and second word are set then emit record to context.
				// Using combination of first letter of each word as the key and 1 as the value.
				if(firstWord.toString().length() > 0 && secondWord.toString().length() > 0){
					// emit the value.
					context.write(new Text(firstWord.toString() + " " + secondWord.toString()), one);
				}
			} // End while
			
			/**
			 * ***********************************
			 * ***********************************
			 * FILL IN CODE FOR THE MAP FUNCTION
			 * ***********************************
			 * ***********************************
			 */
			
			// Use context.write to emit values
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is IntWritable and also must match the datatype of the output value of the map method
	public static class Reduce_One extends Reducer<Text, IntWritable, Text, IntWritable>  {
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
											throws IOException, InterruptedException  {
			int sum = 0;
			for (IntWritable val : values) {
				
				int value = val.get();
				
				/**
				 * **************************************
				 * **************************************
				 * YOUR CODE HERE FOR THE REDUCE FUNCTION
				 * **************************************
				 * **************************************
				 */
				 // Add up all the occurances of the this key.
				 sum += value;
			}
			
			/**
			 * **************************************
			 * **************************************
			 * YOUR CODE HERE FOR THE REDUCE FUNCTION
			 * **************************************
			 * **************************************
			 */
			
			// Use context.write to emit values
			// Emits total number of occurances for the particular bigram.
			context.write(key, new IntWritable(sum));
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, IntWritable>  {
		private ArrayList<String> processed = new ArrayList<String>();
		private Text emitText = new Text();
		private IntWritable emitNum = new IntWritable();

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
										
					/**
					 * ***********************************
					 * ***********************************
					 * FILL IN CODE FOR THE MAP FUNCTION
					 * ***********************************
					 * ***********************************
					 */
					 
					// Tokenize to get the individual words
					String line = value.toString();
					StringTokenizer tokens = new StringTokenizer(line);
					
					while (tokens.hasMoreTokens()) {
						Text bigram = new Text(tokens.nextToken() + " " + tokens.nextToken());
						IntWritable num = new IntWritable(Integer.parseInt(tokens.nextToken()));
						
						// Check if this letter has been processed.
						if(processed.contains(bigram.toString().substring(0,1))){
							// Check if the number of occurances for this letter is larger.
							if(num.get() > emitNum.get()){
								// Update values to emit.
								emitText.set(bigram.toString());
								emitNum.set(num.get());
							}
						}else{
							// Emit the once we already have.
							if(processed.size() > 0){
								context.write(emitText, emitNum);
							}
							// This is the first time we are processing this letter. 
							// Add it to the processed letters.
							processed.add(bigram.toString().substring(0,1));
							emitText.set(bigram.toString());
							emitNum.set(num.get());
						}
					}
					
		}  // End method "map"
		
	}  // End Class Map_Two
	
	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, IntWritable, Text, IntWritable>  {
		
		IntWritable maxInt = new IntWritable();
		
		public void reduce(Text key, IntWritable value, Context context) 
				throws IOException, InterruptedException  {

			// for (Text val : values) {
			// 	/**
			// 	 * **************************************
			// 	 * **************************************
			// 	 * YOUR CODE HERE FOR THE REDUCE FUNCTION
			// 	 * **************************************
			// 	 * **************************************
			// 	 */
			// 	try{
			// 		if(maxInt.get() < Integer.parseInt(val.toString())){
			// 			maxInt.set(Integer.parseInt(val.toString()));
			// 		}
			// 	}
			// 	catch (NumberFormatException nfe) {
			// 		nfe.printStackTrace();
			// 	}
			// }
			
			/**
			 * **************************************
			 * **************************************
			 * YOUR CODE HERE FOR THE REDUCE FUNCTION
			 * **************************************
			 * **************************************
			 */
			 context.write(key, value);
			
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
	
	
	/**
	 * ******************************************************
	 * ******************************************************
	 * YOUR CODE HERE FOR MORE MAP / REDUCE CLASSES IF NEEDED
	 * ******************************************************
	 * ******************************************************
	 */
	
}