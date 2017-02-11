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

public class GraphTriangles extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		// Provide Configuration, CitationGraph AND ARGS.
		int res = ToolRunner.run(new Configuration(), new GraphTriangles(), args);
		System.exit(res); 
	}

	// Run the job with CitationGraph and Configuration and args given.
	public int run ( String[] args ) throws Exception {
		// String input = "/class/s17419/lab3/patents.txt";
		// Perform MapReduce on test sample first.
		String input = "/scr/rvshah/lab3/test_patent.txt";
		String temp1 = "/scr/rvshah/lab3/temp2";
		String output = "/scr/rvshah/lab3/exp2/output/";
		
		// Define number of reducers required for this job.
		int reduce_tasks = 2;
		Configuration conf = new Configuration();

		// Configure job1.
		Job job_one = new Job(conf, "Graph Triangle One"); 
		
		// Set CitationGraph as class.
		job_one.setJarByClass(GraphTriangles.class);
		
		// Set reducers for this task.
		job_one.setNumReduceTasks(reduce_tasks);
		
		// Set Output Key Class.
		job_one.setOutputKeyClass(Text.class); 
		
		// Set Output Value Class.
		job_one.setOutputValueClass(Text.class);
		
		// Set Mapper class.
		job_one.setMapperClass(GraphTriangles_Map_One.class); 
		
		// Set Reducer Class.
		job_one.setReducerClass(GraphTriangles_Reduce_One.class);
		
		// Set Input Format for job 1 to Text Input.
		job_one.setInputFormatClass(TextInputFormat.class);  
		
		// Set output format for job 1 to Text Output.
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		// Add the input path for job 1.
		FileInputFormat.addInputPath(job_one, new Path(input)); 

		// Set output path for job 1.
		FileOutputFormat.setOutputPath(job_one, new Path(temp1));
		
		// Run job 1 and wait for completion.
		job_one.waitForCompletion(true); 
		
		
		Job job_two = new Job(conf, "Graph Triangle Two"); 
		job_two.setJarByClass(CitationGraph.class); 
		job_two.setNumReduceTasks(2); 
		
		job_two.setOutputKeyClass(Text.class); 
		job_two.setOutputValueClass(Text.class);
		
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(GraphTriangles_Map_Two.class); 
		job_two.setReducerClass(GraphTriangles_Reduce_Two.class);
		
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp1)); 
		FileOutputFormat.setOutputPath(job_two, new Path(output));
		
		// Run the job
		job_two.waitForCompletion(true); 

		return 0;
	} // End run
	
	/**
	  * Input:
	  *		KEY: LongWritable - Line numbers / Offset, irrelevent for the mapper.
	  *		VALUE: Text - The Lines of the file.
	  * Output:
	  *		KEY: Text - The patent number that refers to the 'from' part for citation.
	  *		VALUE: Text - The patent number that refers to the 'to' part for citation for one hop citations.
	  */
	public static class GraphTriangles_Map_One extends Mapper<LongWritable, Text, Text, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException  {
			// Conver the line to string and tokenize it.
			String citation = value.toString();
			// Patents array
			String[] patents = citation.split("\\s+");
			// Check if this one hop patent citation is to itself, if so ignore it. We do not need to follow the citations from-to the same patent.
			if(!patents[0].equals(patents[1])){
				context.write(new Text(patents[1]), new Text(patents[0]));
				context.write(new Text(patents[0]), new Text(patents[1]));
			}
		} // End method "map"
	} // End Class GraphTriangles_Map_One
	
	/**
	  * GraphTriangles_Reduce_One class
	  * Input:
	  *		KEY: Text - Middle node of the triplets
	  *		VALUE: Iteratble<Text> - The list of the nodes key is connected to.
	  * Output:
	  *		KEY: Text - The patent key that refers to the patent that we are computing the significance of.
	  *		VALUE: Text - The patent value that refers to the patent that has either one or two hop citation to the key patent.
	  */
	public static class GraphTriangles_Reduce_One extends Reducer<Text, Text, Text, Text>  {
		private Text keyNode = new Text();
		final static LongWritable zero = new LongWritable((byte)0);
		ArrayList<IntWritable> valueArrayList = new ArrayList<IntWritable>();

		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException  {
			// We need to combine one hops citations.
			String value = null;
			boolean first = true;
			// Iterate over all values for this key. The values are the patents that has one citation to the 'key'.
			for(Text val: values){
				if(first){
					value = val.toString();
					first = false;
				}else{
					value += "," + val.toString();
				}
			}
			context.write(key,new Text(value));
		} // End method "reduce" 
	} // End Class GraphTriangles_Reduce_One
	
	/**
	  * GraphTriangles_Map_Two class
	  * Input:
	  *		KEY: LongWritable - Pantent number key that was the output from first round.
	  *		VALUE: Text - Patent number value that was the output of key from first round.
	  * Output:
	  *		KEY: Text - The patent number that refers to the 'from' part for citation.
	  *		VALUE: Text - The patent number that refers to the 'to' part for citation for one hop citations.
	  */
	public static class GraphTriangles_Map_Two extends Mapper<LongWritable, Text, Text, Text>  {
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException  {
				String[] input = value.toString().split("\\s+");
				String vertex_id = input[0];
				String neighbor_list = input[1];
				String[] neighbors = neighbor_list.split(",");
				for(String neighbor : neighbors){
					context.write(new Text(neighbor), new Text(vertex_id + ":" + neighbor_list));
				}
		}  // End method "map"
	}  // End Class GraphTriangles_Map_Two
	
	/**
	  * GraphTriangles_Reduce_Two class
	  * Input:
	  *		KEY: Text - Distinct Patent numbers from Map_Two class.
	  *		VALUE: Iteratble<Text> - The list of the patents numbers that cites key patent has been groupped together by the key patent number.
	  * Output:
	  *		KEY: Text - The patent key that refers to the patent that we are computing the significance of.
	  *		VALUE: Text - Format: vertex_id:neighbor_list
	  */
	public static class GraphTriangles_Reduce_Two extends Reducer<Text, Text, Text, Text>  {
		private ArrayList<String> triplets = new ArrayList<String>();
		private int numberOfTriangles = 0;
		private int numberOfTriplets = 0;
		public void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException  {
				// Construct 2-Neighborhood graph of vertex_id and enumarate all Triangles in this graph.
				for(Text val : value){
					String[] values = val.toString().split(":");
					String vertex_id = values[0];
					String[] neighbor_list = values[1].split(",");
					if(neighbor_list.length > 1){
						for(int i=0; i<neighbor_list.length-1;i++){
							triplets.add(neighbor_list[i]+"-"+vertex_id+"-"+neighbor_list[i+1]);
							triplets.add(neighbor_list[i+1]+"-"+vertex_id+"-"+neighbor_list[i]);
							numberOfTriplets++;
						}
						triplets.add(neighbor_list[0]+"-"+vertex_id+"-"+neighbor_list[neighbor_list.length-1]);
						triplets.add(neighbor_list[neighbor_list.length-1]+"-"+vertex_id+"-"+neighbor_list[0]);
						numberOfTriplets++;
					}
				}
		}  // End method "reduce"

		@Override
		protected void cleanup(Context context)
			throws IOException, InterruptedException {
			// Compute the number of Triangles.
			String[] t = null;
			for(int i=0;i<triplets.size()-3;i++){
				t = triplets.get(i).split("-");
				if((triplets.contains(t[1]+t[2]+t[0]) || triplets.contains(t[0]+t[2]+t[1])) &&
					(triplets.contains(t[2]+t[0]+t[1]) || triplets.contains(t[1]+t[0]+t[2])) &&
					(triplets.contains(t[2]+t[1]+t[0]) || triplets.contains(t[0]+t[1]+t[2])) ){
					// This must be a triangle.
					numberOfTriangles++;
				}
			}
			context.write(new Text("Num Triplets"), new Text(Integer.toString(numberOfTriplets)));
			context.write(new Text("Num Triangles"), new Text(Integer.toString(numberOfTriangles)));
			context.write(new Text("GCC Value"), new Text(Double.toString((3*numberOfTriangles)/numberOfTriplets)));
		}
	}  // End Class GraphTriangles_Reduce_Two
}