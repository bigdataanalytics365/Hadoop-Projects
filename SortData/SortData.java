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

public class SortData extends Configured implements Tool {
	public static void main ( String[] args ) throws Exception {
		// Provide Configuration, CitationGraph AND ARGS.
		int res = ToolRunner.run(new Configuration(), new SortData(), args);
		System.exit(res); 
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
		return -1;
		}
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		FeaturesSequenceFileOutputFormat.setOutputCompressionType(job,
		CompressionType.BLOCK);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		InputSampler.Sampler<IntWritable, Text> sampler =
		new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
		InputSampler.writePartitionFile(job, sampler);
		// Add to DistributedCache
		Configuration conf = job.getConfiguration();
		String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		URI partitionUri = new URI(partitionFile);
		job.addCacheFile(partitionUri);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}