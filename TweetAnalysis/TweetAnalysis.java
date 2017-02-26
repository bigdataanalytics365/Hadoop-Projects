import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

public class TweetAnalysis {
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        int reduce_tasks = 4;
        
        // Create a Hadoop Job
        Job job = Job.getInstance(conf, "CustomInputFormat using MapReduce");
        
        // Attach the job to this Class
        job.setJarByClass(TweetAnalysis.class); 
        
        // Number of reducers
        job.setNumReduceTasks(reduce_tasks);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(Text.class);
        
        // Set the Map class
        job.setMapperClass(Map.class); 
        job.setReducerClass(Reduce.class);
        
        // Set how the input is split
        // TextInputFormat.class splits the data per line
        job.setInputFormatClass(JSONInputFormat.class); 
        
        // Output format class
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Input path
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
        
        // Output path
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        // Run the job
        job.waitForCompletion(true);
    } 

    public static void main ( String[] args ) throws Exception {
		// Provide Configuration, CitationGraph AND ARGS.
		int res = ToolRunner.run(new Configuration(), new SortData(), args);
		System.exit(res); 
	}

	// Run the job with CitationGraph and Configuration and args given.
	public int run ( String[] args ) throws Exception {
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
	  *		KEY: 
	  *		VALUE: 
	  * Output:
	  *		KEY: 
	  *		VALUE: 
	  */
	public static class ConvertToSequenceFileMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outkey = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		}
	}

    /**
	  * JSONInputFormat class
	  * This class is a returns a new JSONReader object.
	  */
    public static class JSONInputFormat extends FileInputFormat <LongWritable, Text> {
        @Override 
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) 
            throws IOException, InterruptedException {
            return new JSONReader();
        }
    }
    
    /**
	  * JSONReader class : extends the RecordReader class
	  * This class simply reads a JSON object from the file one at a time.
	  */
    public static class JSONReader extends RecordReader <LongWritable, Text> {
        private LineReader lineReader;
        private LongWritable key;
        private Text value;
        long start, end, position, no_of_calls;

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            
            Configuration conf = context.getConfiguration();
            
            FileSplit split = (FileSplit) genericSplit;
            final Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);
            
            start = split.getStart();
            end = start + split.getLength();
            position = start;
            
            FSDataInputStream input_file = fs.open(split.getPath());
            input_file.seek(start);
                                                                
            lineReader = new LineReader(input_file, conf); 
            
            no_of_calls = 0;
        }  

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (start == end) {
                return 0.0f;
            }else{
                return Math.min(1.0f, (position - start) / (float)(end - start));
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override                                                
        public boolean nextKeyValue() throws IOException {
            no_of_calls = no_of_calls + 1;
            if (position == end)  {
                return false;
            }
            if (key == null) {
                key = new LongWritable();
            }            
            if (value == null) {
                value = new Text(" ");
            }
            key.set(no_of_calls);
            Text temp_text = new Text(" ");
            int read_length = lineReader.readLine(temp_text);
            String temp = temp_text.toString();
            position = position + read_length;
            read_length = lineReader.readLine(temp_text);
            temp = temp + temp_text.toString();
            position = position + read_length;
            value.set(temp);
            return true;
        }

        @Override
        public void close() throws IOException {
            if ( lineReader != null )
                lineReader.close();
        }
    }
}