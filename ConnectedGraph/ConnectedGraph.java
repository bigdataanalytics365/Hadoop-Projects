import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;
import java.util.*;
import com.google.gson.*;

// NOTE: The following code works on the sample file. 
// I have took the first ~50,000 from the university.json file and run it locally. It works and output is attached in the report.
// Though when running the same code on cystorm cluster, it kept throwing JsonMalformed error without any useful trace to debug.

/**
 * TweetAnalysis class
 */
public class ConnectedGraph {
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // String input = "/class/s17419/lab5/university.json";
        // String hashTagOutput = "/scr/rvshah/lab5/exp1/output/";
        // String followerOutput = "/scr/rvshah/lab5/exp2/output/";
        // String profilicOutput = "/scr/rvshah/lab5/exp3/output/";

        String input = "/home/rushabhs/Desktop/CPRE/419/lab6/test.txt";
        String output = "/home/rushabhs/Desktop/CPRE/419/lab6/output";
        
        int reduce_tasks = 1;
        
        // Create a Hadoop Job
        Job connectedGraphJob = Job.getInstance(conf, "Connected Graph Job");
        
        // Attach the job to this Class
        connectedGraphJob.setJarByClass(ConnectedGraph.class); 
        
        // Number of reducers
        connectedGraphJob.setNumReduceTasks(reduce_tasks);
        
        connectedGraphJob.setMapOutputKeyClass(Text.class);
        connectedGraphJob.setMapOutputValueClass(Text.class);
        connectedGraphJob.setOutputKeyClass(Text.class); 
        connectedGraphJob.setOutputValueClass(Text.class);
        
        // Set the Map class
        connectedGraphJob.setMapperClass(GraphMapper.class); 
        connectedGraphJob.setReducerClass(GraphReducer.class);
        
        connectedGraphJob.setInputFormatClass(TextInputFormat.class); 
        
        // Output format class
        connectedGraphJob.setOutputFormatClass(TextOutputFormat.class);
        
        // Input path
        FileInputFormat.addInputPath(connectedGraphJob, new Path(input)); 
        
        // Output path
        FileOutputFormat.setOutputPath(connectedGraphJob, new Path(output));
        
        // Run the job
        connectedGraphJob.waitForCompletion(true);
    } 

    /**
	  * GraphMapper class
	  * 
	  * Input:
	  *		KEY: File offset
	  *		VALUE: The JSON string read as custom input.
	  * Output:
	  *		KEY: hashtags
	  *		VALUE: 1
	  */
    public static class GraphMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
            context.write(new Text(tag), new IntWritable(1));
        }
    }

    /**
	  * GraphReducer class
	  * 
	  * Input:
	  *		KEY: hashtags
	  *		VALUE: 1
	  * Output:
	  *		KEY: hashtags
	  *		VALUE: # of times 
	  */
    public static class GraphReducer extends Reducer<Text, IntWritable, Text, Text>  {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
            int count = 0;
            for (IntWritable val : values) {
                count++;
            }
            context.write(key, new Text(Integer.toString(count)));
        }
    }

    /**
	  * ConnectedGraphMapper class
	  * 
	  * Input:
	  *		KEY: File offset
	  *		VALUE: The JSON string read as custom input.
	  * Output:
	  *		KEY: Screen_name from the 'user' JsonObject.
	  *		VALUE: Followers_count from the 'user' JsonObject
	  */
    public static class ConnectedGraphMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
            context.write(new Text(user.getAsJsonPrimitive("screen_name").getAsString()), new IntWritable(user.getAsJsonPrimitive("followers_count").getAsInt()));
        }
    }
    
    /**
	  * ConnectedGraphReducer class
	  * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	  * 
	  * Input:
	  *		KEY: Screen_name
	  *		VALUE: followers_count list of this screen_name
	  * Output:
	  *		KEY: Screen_name
	  *		VALUE: Max followers_count from the list of followers_count.
	  */
    public static class ConnectedGraphReducer extends Reducer<Text, IntWritable, Text, Text>  {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
            int maxFollower = 0;
            for (IntWritable val : values) {
            }
            context.write(key, new Text(Integer.toString(maxFollower)));
        }
    }
}