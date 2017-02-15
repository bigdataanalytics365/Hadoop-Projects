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
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;

public class SortData extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		// Provide Configuration, CitationGraph AND ARGS.
		int res = ToolRunner.run(new Configuration(), new SortData(), args);
		System.exit(res); 
	}

	// Run the job with CitationGraph and Configuration and args given.
	public int run ( String[] args ) throws Exception {
		// String input = "/class/s17419/lab4/gensort-out-500K";
		// String input = "/class/s17419/lab4/gensort-out-5M";
		String input = "/class/s17419/lab4/gensort-out-50M";
		String temp = "/scr/rvshah/lab4/temp";
		String output = "/scr/rvshah/lab4/exp1/output/";
		int reduce_tasks = 4;

		Configuration conf = new Configuration();

		// This is a map only job
		Job convertToSequenceJob = new Job(conf, "Sequence Conversion");
		convertToSequenceJob.setJarByClass(SortData.class);
		convertToSequenceJob.setNumReduceTasks(0);
		convertToSequenceJob.setMapperClass(ConvertToSequenceFileMapper.class);
		
		convertToSequenceJob.setOutputKeyClass(Text.class);
		convertToSequenceJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(convertToSequenceJob, new Path(input));
		FileOutputFormat.setOutputPath(convertToSequenceJob, new Path(temp));
		convertToSequenceJob.setInputFormatClass(TextInputFormat.class);
		convertToSequenceJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		SequenceFileOutputFormat.setCompressOutput(convertToSequenceJob, true);
		SequenceFileOutputFormat.setOutputCompressorClass(convertToSequenceJob, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(convertToSequenceJob, CompressionType.BLOCK);
		
		convertToSequenceJob.waitForCompletion(true);
		
		Job sortJob = new Job(conf, "Sortdata Job");
		sortJob.setJarByClass(SortData.class);
		sortJob.setNumReduceTasks(reduce_tasks);
		sortJob.setOutputKeyClass(Text.class);
		sortJob.setOutputValueClass(Text.class);
		sortJob.setInputFormatClass(SequenceFileInputFormat.class);
		sortJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(sortJob, new Path(temp));
		FileOutputFormat.setOutputPath(sortJob, new Path(output));
		
		sortJob.setPartitionerClass(TotalOrderPartitioner.class);
		
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1,10000,10);
		InputSampler.writePartitionFile(sortJob, sampler);
		
		// Add to the DistributedCache
		Configuration config = sortJob.getConfiguration();
		String partitionFile = TotalOrderPartitioner.getPartitionFile(config);
		URI partitionUri = new URI(partitionFile);
		sortJob.addCacheFile(partitionUri);
		
		sortJob.setReducerClass(ConvertToTextFileReducer.class);
		
		sortJob.waitForCompletion(true);
		return 0;
	}
	
	/**
	  * ConvertToSequenceFileMapper class
	  * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	  * 
	  * Input:
	  *		KEY: LongWritable - Line numbers / Offset, irrelevent for the mapper.
	  *		VALUE: Text - The Lines of the file.
	  * Output:
	  *		KEY: Text - The first 10 characters of the line as they were read from the input file.
	  *		VALUE: Text - This contains the entire line of text including the key. Thus looking at the final output you will see key twice.
	  *			The first key refers to the actual key of map reduce, the second key is associated with the value portion for the key.
	  *			For more look at the Mapper class below.
	  */
	public static class ConvertToSequenceFileMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outkey = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String invalue = value.toString();
			if (invalue != null) {
				outkey.set(invalue.substring(0, 10));
				// If you do not want to see the key twice, then uncomment line #108 and comment out the line #106.
				context.write(outkey, value);
				// The following line will get rid of the the 'key' portion in the values of the output.
				// context.write(outkey, new Text(invalue.substring(10, invalue.length())));
			}
		}
	}

	/**
	  * ConvertToTextFileReducer class
	  * This class simply outputs the input as it is given without any modification..
	  * 
	  * Input/Output:
	  *		KEY: Text - The first 10 characters of the line as they were read from the input file.
	  *		VALUE: Text - Depending upon the mapper class above, either the entire line as the value or the value with key stripped from it.
	  */
	public static class ConvertToTextFileReducer extends Reducer<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
}
