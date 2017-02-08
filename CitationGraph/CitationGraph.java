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
		// Provide Configuration, CitationGraph AND ARGS.
		int res = ToolRunner.run(new Configuration(), new CitationGraph(), args);
		System.exit(res); 
	}

	// Run the job with CitationGraph and Configuration and args given.
	public int run ( String[] args ) throws Exception {
		// String input = "/class/s17419/lab3/patents.txt";
		// Perform MapReduce on test sample first.
		String input = "/home/rvshah/test_patent.txt";
		String temp = "/scr/rvshah/lab3/exp1/temp";
		String output = "/scr/rvshah/lab3/exp1/output/";
		
		// Define number of reducers required for this job.
		int reduce_tasks = 1;
		Configuration conf = new Configuration();

		// Configure job1.
		Job job_one = new Job(conf, "Citation Graph Round One"); 
		
		// Set CitationGraph as class.
		job_one.setJarByClass(CitationGraph.class);
		
		// Set reducers for this task.
		job_one.setNumReduceTasks(reduce_tasks);
		
		// Set Output Key Class.
		job_one.setOutputKeyClass(Text.class); 
		
		// Set Output Value Class.
		job_one.setOutputValueClass(Text.class);
		
		// Set Mapper class.
		job_one.setMapperClass(One_Hop_Citation_Mapper.class); 
		
		// Set Reducer Class.
		job_one.setReducerClass(One_Hop_Citation_Reducer.class);
		
		// Set Input Format for job 1 to Text Input.
		job_one.setInputFormatClass(TextInputFormat.class);  
		
		// Set output format for job 1 to Text Output.
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		// Add the input path for job 1.
		FileInputFormat.addInputPath(job_one, new Path(input)); 

		// Set output path for job 1.
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		
		// Run job 1 and wait for completion.
		job_one.waitForCompletion(true); 
		
		return 0;
	} // End run
	
	/**
	  * One_Hop_Citation_Mapper class
	  * This class will map each patent as read in from the input class and
	  * map it to patent with one hop citation.
	  * Input:
	  *		KEY: LongWritable - Line numbers, irrelevent for the mapper.
	  *		VALUE: Text - The Lines of the file.
	  * Output:
	  *		KEY: Text - The patent number that refers to the 'from' part for citation.
	  *		VALUE: Text - The patent number that refers to the 'to' part for citation for one hop citations.
	  */
	public static class One_Hop_Citation_Mapper extends Mapper<LongWritable, Text, Text, Text>  {
		// The map method.
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException  {
			// Conver the line to string and tokenize it.
			String citation = value.toString();
			// Tokenize to get two distinct patents.
			StringTokenizer patents = new StringTokenizer(citation);
			// Check if this one hop patent citation is to itself, if so ignore it. We do not need to follow the citations from-to the same patent.
			if(!patents[0].equals(patents[1])){
				// This is the one hop patent citation pair. Emit it in reverse fashion.
				// The input format is that : [From node] -> [To node]
				// We need to reverse this, which would map the [To node] -> [From node], since the significance of a patetnt is the number of distinct patents that cites it.
				// In other words it looks at the incoming edge to determine whether it should count towards the significance.
				// Emit the patent pair.
				context.write(new Text(patents[1]), new Text(patents[0]));
			}
		} // End method "map"
	} // End Class One_Hop_Citation_Mapper
	
	/**
	  * One_Hop_Citation_Reducer class
	  * This class will be given the results of One_Hop_Citation_Mapper.
	  * Input:
	  *		KEY: Text - Distinct Patent Numbers extracted from the file,
	  *		VALUE: Iteratble<Text> - The list of the patent numbers that has been groupped together by the key patent number.
	  * Output:
	  *		KEY: Text - The patent number that refers to the 'from' part for citation.
	  *		VALUE: Text - The patent number that refers to the 'to' part for citation for both, one and two hop, citations.
	  */
	public static class One_Hop_Citation_Reducer extends Reducer<Text, Text, Text, Text>  {
		private HashMap<String, ArrayList<String>> citations = new HashMap<String, ArrayList<String>>();
		private ArrayList<String> processedKeys = new ArrayList<String>();

		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException  {
			// We need to combine one and two hops citations. Which will be stored in hashmap defined above.
			String patentKey = key.toString();
			// processedKeys = new ArrayList<String>();			
			// processedKeys.addAll(citations.keySet());
			// Iterate over all values for this key. The values are the patents that has a citation to the 'key'.
			for(Text val: values){
				// This is the 'to' part of the citation. Convert it to string and then process it.
				String patent = val.toString();
				if(processedKeys.contains(patent)){
					// This patent has been processed already. Emit the patent as key and patentKey as value.
					// Because this denotes that there is a citation from patent to patentKey.
					context.write(new Text(patent), new Text(patentKey));
					// (citations.get(patent)).add(patentKey);
				}else{
					// Mark this patent as processed
					processedKeys.add(patent);
					context.write(new Text(patent), new IntWritable((citations.get(patent)).size()));					
					// This key has not been processed yet. We need to create a new record and put it in citations HashMap.
					ArrayList<String> temp_patents = new ArrayList<String>();
					// Add the patent
					temp_patents.add(patentKey);
					citations.put(patent,temp_patents);
				} // End if-else the 'val' being processed is in processedKeys.
				// Emit the patent / node name as key and the val as the value.
				context.write(new Text(patent), val);
			} // End looping through all possible values for this reduce method.
		} // End method "reduce" 
		
		// We have gone through all key->value output of map. Now go through the citations that we have recorded and emit them.
		for(String patent : citations.keySet()){
			// Emit the patent / node name as key and the size of the arraylist that contains the patents that cites the key node.
			context.write(new Text(patent), new IntWritable((citations.get(patent)).size()));
		} // End lopping thorugh all the patents.
		
	} // End Class One_Hop_Citation_Reducer
	
	// // The second Map Class
	// public static class Map_Two extends Mapper<LongWritable, Text, Text, IntWritable>  {
	// 	public void map(LongWritable key, Text value, Context context) 
	// 		throws IOException, InterruptedException  {
	// 	}  // End method "map"
	// }  // End Class Map_Two
	// 
	// // The second Reduce class
	// public static class Reduce_Two extends Reducer<Text, IntWritable, Text, IntWritable>  {
	// 	IntWritable maxInt = new IntWritable();
	// 	public void reduce(Text key, IntWritable value, Context context) 
	// 			throws IOException, InterruptedException  {
	// 	}  // End method "reduce"
	// }  // End Class Reduce_Two
	// for each value in the set(values) in reducers
	//     if that key exists and has been processed
	//         add this key to the list of that processed key
	//     else: this key has not been processed yet
	//         add it a new item in the list and add this key as value to the list of that processed key.
	// 
	// 
	// 
	// String patent = val.toString();
	// if(processedKeys.contains(patent)){
	//     // This key has been processed. Just add this  reducer's key to the citations's ArrayList.
	//     (citations.get(patent)).add(patentKey);
	// }else{
	//     // This key has not been processed yet. We need to create a new record and put it in citations HashMap.
	//     ArrayList<String> temp_patents = new ArrayList<String>();
	//     // Add the patent
	//     temp_patents.add(patentKey);
	//     citations.put(patent,temp_patents);
	//     // Mark this patent as processed
	//     processedKeys.add(patent);
	// } // End if-else the 'val' being processed is in processedKeys.	
}