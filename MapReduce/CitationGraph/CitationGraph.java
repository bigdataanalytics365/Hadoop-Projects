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
		String input = "/class/s17419/lab3/patents.txt";
		// Perform MapReduce on test sample first.
		// String input = "/scr/rvshah/lab3/test_patent.txt";
		String temp = "/scr/rvshah/lab3/temp";
		String output = "/scr/rvshah/lab3/exp1/output/";
		
		// Define number of reducers required for this job.
		int reduce_tasks = 2;
		Configuration conf = new Configuration();

		// Configure job1.
		Job job_one = new Job(conf, "Citation Graph One"); 
		
		// Set CitationGraph as class.
		job_one.setJarByClass(CitationGraph.class);
		
		// Set reducers for this task.
		job_one.setNumReduceTasks(reduce_tasks);
		
		// Set Output Key Class.
		job_one.setOutputKeyClass(Text.class); 
		
		// Set Output Value Class.
		job_one.setOutputValueClass(Text.class);
		
		// Set Mapper class.
		job_one.setMapperClass(Citation_Graph_Map_One.class); 
		
		// Set Reducer Class.
		job_one.setReducerClass(Citation_Graph_Reduce_One.class);
		
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
		
		
		Job job_two = new Job(conf, "Citation Graph Two"); 
		job_two.setJarByClass(CitationGraph.class); 
		job_two.setNumReduceTasks(1); 
		
		job_two.setOutputKeyClass(Text.class); 
		job_two.setOutputValueClass(Text.class);
		
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Citation_Graph_Map_Two.class); 
		job_two.setReducerClass(Citation_Graph_Reduce_Two.class);
		
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp)); 
		FileOutputFormat.setOutputPath(job_two, new Path(output));
		
		// Run the job
		job_two.waitForCompletion(true); 

		return 0;
	} // End run
	
	/**
	  * One_Hop_Citation_Mapper class
	  * This class will map the second patent of the line to the first line as it denotes the one hop citation.
	  * The first patent contributes to the significance of the second patent, so long as they are not the same.
	  * It will exclude those lines where first patent = second patent, as per requirements.
	  * 
	  * Input:
	  *		KEY: LongWritable - Line numbers / Offset, irrelevent for the mapper.
	  *		VALUE: Text - The Lines of the file.
	  * Output:
	  *		KEY: Text - The patent number that refers to the 'from' part for citation.
	  *		VALUE: Text - The patent number that refers to the 'to' part for citation for one hop citations.
	  */
	public static class Citation_Graph_Map_One extends Mapper<LongWritable, Text, Text, Text>  {
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException  {
			// Conver the line to string and tokenize it.
			String citation = value.toString();
			// Tokenize to get two distinct patents.
			StringTokenizer patentsTokenizer = new StringTokenizer(citation);
			// Patents array
			String[] patents = citation.split("\\s+");
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
	} // End Class Citation_Graph_Map_One
	
	/**
	  * Citation_Graph_Reduce_One class
	  * This class will be given the results of Citation_Graph_Map_One. 
	  * The input key is the distinct patents for which we have computed the one hop citations.
	  * The input values are the set of patents that contribute to the significance of the key patent.
	  * Input:
	  *		KEY: Text - Distinct Patent Numbers extracted from the file,
	  *		VALUE: Iteratble<Text> - The list of the patent numbers that has been groupped together by the key patent number.
	  * Output:
	  *		KEY: Text - The patent key that refers to the patent that we are computing the significance of.
	  *		VALUE: Text - The patent value that refers to the patent that has either one or two hop citation to the key patent.
	  */
	public static class Citation_Graph_Reduce_One extends Reducer<Text, Text, Text, Text>  {
		private HashMap<String, LinkedList<String>> citations = new HashMap<String, LinkedList<String>>();

		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException  {
				
			LinkedList<String> patentValues = new LinkedList<String>();
			// We need to combine one hops citations.
			String patentKey = key.toString();
			// Iterate over all values for this key. The values are the patents that has one citation to the 'key'.
			for(Text val: values){
				// Immediately emit the key, val pair because this is the one hop citation that was computed during the One_Hop_Citation_Mapper.
				String patentVal = val.toString();
				boolean contains = true;
				if(!patentValues.contains(patentVal)){
					contains = false;
					patentValues.add(patentVal);
					context.write(key,val);
				}
				if(citations.containsKey(patentVal)){
					// This patent has been processed and is available in citations HashMap.
					// We need to emit patentKey, patentVal because the patentVal has two hop citation to patentKey.
					LinkedList<String> contributers = citations.get(patentVal);
					for(String patent: contributers){
						// Ensure patent != patentKey and patent has not been emitted already.
						if((!patent.equals(patentKey)) && (!contains) && (!patent.equals(patentVal))){
							patentValues.add(patent);
							context.write(key, new Text(patent));
						}
					}
				} // End if patentVal has been processed.
			} // End looping through all possible values for this reduce method.
			// Add patentKey to citations.
			citations.put(patentKey,patentValues);
		} // End method "reduce" 
	} // End Class Citation_Graph_Reduce_One
	
	/**
	  * Citation_Graph_Map_Two class
	  * This class will map the second patent of the line to the first line as it denotes the one hop citation.
	  * The first patent contributes to the significance of the second patent, so long as they are not the same.
	  * It will exclude those lines where first patent = second patent, as per requirements.
	  * 
	  * Input:
	  *		KEY: LongWritable - Pantent number key that was the output from first round.
	  *		VALUE: Text - Patent number value that was the output of key from first round.
	  * Output:
	  *		KEY: Text - The patent number that refers to the 'from' part for citation.
	  *		VALUE: Text - The patent number that refers to the 'to' part for citation for one hop citations.
	  */
	public static class Citation_Graph_Map_Two extends Mapper<LongWritable, Text, Text, Text>  {
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException  {
				// Just emit the key value
				String[] patents = value.toString().split("\\s+");
				context.write(new Text(patents[0]),new Text(patents[1]));
		}  // End method "map"
	}  // End Class Citation_Graph_Map_Two
	
	/**
	  * Citation_Graph_Reduce_Two class
	  * This class will be given the results of  
	  * The input key is the distinct patents for which we have computed the one hop citations.
	  * The input values are the set of patents that contribute to the significance of the key patent.
	  * Input:
	  *		KEY: Text - Distinct Patent numbers from Map_Two class.
	  *		VALUE: Iteratble<Text> - The list of the patents numbers that cites key patent has been groupped together by the key patent number.
	  * Output:
	  *		KEY: Text - The patent key that refers to the patent that we are computing the significance of.
	  *		VALUE: Text - The patent value that refers to the patent that has either one or two hop citation to the key patent.
	  */
	public static class Citation_Graph_Reduce_Two extends Reducer<Text, Text, Text, IntWritable>  {
		HashMap<String, Integer> patents = new HashMap<String, Integer>();
		public void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException  {
				String patentKey = key.toString();
				int count = 0;
				for(Text val: value){
					count++;
				}
				if(!patents.containsKey(patentKey)){
					// Put this key and number of citations in patent.
					patents.put(patentKey,count);
				}
		}  // End method "reduce"
		
		@Override
		protected void cleanup(Context context)
			throws IOException, InterruptedException {
			// Perform cleanup here.
			CountComparator bvc = new CountComparator(patents);
        	TreeMap<String, Integer> sortedPatent = new TreeMap<String, Integer>(bvc);
			sortedPatent.putAll(patents);
			int count = 0;
			for(Map.Entry<String, Integer> entry: sortedPatent.entrySet()){
				if(count < 10){
					context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
					count++;
				}else{
					return;
				}
			}
		}
		
		class CountComparator implements Comparator<String> {
		    Map<String, Integer> p;

		    public CountComparator(Map<String, Integer> p) {
		        this.p = p;
		    }

		    // Note: this comparator imposes orderings that are inconsistent with
		    // equals.
		    public int compare(String a, String b) {
		        if (p.get(a) >= p.get(b)) {
		            return -1;
		        } else {
		            return 1;
		        } // returning 0 would merge keys
		    }
		}
	}  // End Class Citation_Graph_Reduce_Two
}