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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;
import java.util.*;
import com.google.gson.*;

public class TweetAnalysis {
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // String input = "/class/s17419/lab5/university.json";
        // String output = "/scr/rvshah/lab5/exp1/output/";
        String input = "/home/rushabhs/Desktop/CPRE/419/lab5/sample.json";
        String output = "/home/rushabhs/Desktop/CPRE/419/lab5/output";
        
        int reduce_tasks = 1;
        
        // Create a Hadoop Job
        Job hashtagJob = Job.getInstance(conf, "Top Hashtags Job");
        
        // Attach the job to this Class
        hashtagJob.setJarByClass(TweetAnalysis.class); 
        
        // Number of reducers
        hashtagJob.setNumReduceTasks(reduce_tasks);
        
        hashtagJob.setMapOutputKeyClass(Text.class);
        hashtagJob.setMapOutputValueClass(IntWritable.class);
        hashtagJob.setOutputKeyClass(Text.class); 
        hashtagJob.setOutputValueClass(Text.class);
        
        // Set the Map class
        hashtagJob.setMapperClass(HashtagMapper.class); 
        hashtagJob.setReducerClass(HashtagReducer.class);
        
        // Set how the input is split
        // TextInputFormat.class splits the data per line
        hashtagJob.setInputFormatClass(JSONInputFormat.class); 
        
        // Output format class
        hashtagJob.setOutputFormatClass(TextOutputFormat.class);
        
        // Input path
        FileInputFormat.addInputPath(hashtagJob, new Path(input)); 
        
        // Output path
        FileOutputFormat.setOutputPath(hashtagJob, new Path(output));
        
        // Run the job
        hashtagJob.waitForCompletion(true);
    } 

    /**
	  * Map class
	  * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	  * 
	  * Input:
	  *		KEY: File offset
	  *		VALUE: The JSON string read as custom input.
	  * Output:
	  *		KEY: hashtags
	  *		VALUE: 1
	  */
    public static class HashtagMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
            JsonParser parser = new JsonParser();
            JsonObject tweet = parser.parse(value.toString()).getAsJsonObject();
            JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
            Map<String, Integer> tags = new HashMap<String, Integer>();
            if(hashtags != null && hashtags.size() > 0){
                for(JsonElement tag: hashtags){
                    JsonObject temp = tag.getAsJsonObject();
                    String tagText = temp.get("text").getAsString();
                    tags.put(tagText,1);
                }
                for(String tag: tags.keySet()){
                    context.write(new Text(tag), new IntWritable(1));
                }
            }
        }
    }

    /**
	  * Reduce class
	  * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	  * 
	  * Input:
	  *		KEY: hashtags
	  *		VALUE: 1
	  * Output:
	  *		KEY: hashtags
	  *		VALUE: # of times 
	  */
    public static class HashtagReducer extends Reducer<Text, IntWritable, Text, Text>  {
        private HashMap<Text, Integer> topTenHashTags = new HashMap<Text, Integer>();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
            int count = 0;
            for (IntWritable val : values) {
                count++;
            }
            // topTenHashTags.put(key, count);
            context.write(key, new Text(Integer.toString(count)));
        }
    }

    // /**
	//   * Map class
	//   * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	//   * 
	//   * Input:
	//   *		KEY: File offset
	//   *		VALUE: The JSON string read as custom input.
	//   * Output:
	//   *		KEY: hashtags
	//   *		VALUE: 1
	//   */
    // public static class HashtagMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
    //     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
    //         JsonParser parser = new JsonParser();
    //         JsonElement tweetElement = parser.parse(value.toString());
    //         if(tweetElement.isJsonObject()){
    //             JsonObject tweet = tweetElement.getAsJsonObject();
    //             JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
    //             Map<String, Integer> tags = new HashMap<String, Integer>();
    //             if(hashtags != null){
    //                 for(JsonElement tag: hashtags){
    //                     JsonObject temp = tag.getAsJsonObject();
    //                     String tagText = temp.get("text").getAsString();
    //                     tags.put(tagText,1);
    //                 }
    //                 for(String tag: tags.keySet()){
    //                     context.write(new Text(tag), new IntWritable(1));
    //                 }
    //             }
    //         }
    //     }
    // }
    // 
    // /**
	//   * Reduce class
	//   * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	//   * 
	//   * Input:
	//   *		KEY: hashtags
	//   *		VALUE: 1
	//   * Output:
	//   *		KEY: hashtags
	//   *		VALUE: # of times 
	//   */
    // public static class HashtagReducer extends Reducer<Text, IntWritable, Text, Text>  {
    //     private TreeMap<Integer, Text> topTenHashTags = new TreeMap<Integer, Text>(Collections.reverseOrder());
    //     public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
    //         int count = 0;
    //         for (IntWritable val : values) {
    //             count++;
    //         }
    //         topTenHashTags.put(count, key);
    //     }
    //     
    //     @Override
    //     protected void cleanup(Context context) throws IOException, InterruptedException {
    //         int count = 0;
    //         for(Map.Entry<Integer,Text> entry : topTenHashTags.entrySet()) {
    //             if(count == 10){
    //                 break;
    //             }else{
    //                 count++;
    //             }
    //             context.write(entry.getValue(), new Text(Integer.toString(entry.getKey())));
    //         }
    //     }
    // }

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
            StringBuilder jsonText = new StringBuilder();
            boolean jsonIncomplete = true;
            while(jsonIncomplete){
                int read_length = lineReader.readLine(temp_text);
                String temp = temp_text.toString();
                if(!temp.isEmpty()){
                    temp.replaceAll("\\s+","");
                    if(temp.equals("}") || position == end){
                        jsonIncomplete = false;
                    }else if(temp.equals("},")){
                        jsonIncomplete = false;
                        temp = temp.substring(0,temp.length()-1);
                    }else if(temp.equals("[") || temp.equals("]")){
                        if(temp.equals("]")){
                            jsonIncomplete = false;
                            temp_text = null;
                        }else{
                            temp = temp.substring(1,temp.length());
                        }
                    }
                    if(temp_text != null){
                        jsonText.append(temp);
                    }
                    position = position + read_length;
                }
            }
            if(temp_text != null){
                value.set(jsonText.toString());

                // System.out.println("Value is " + jsonText.toString());
                // System.out.println((new Gson().fromJson(value.toString(),JsonElement.class)).getClass().getName());

                // value.set(new Gson().toJson(jsonText.toString()));
                // if((new JsonParser().parse(jsonText.toString())).isJsonObject()){
                //     JsonObject tweet = new JsonParser().parse(jsonText.toString()).getAsJsonObject();
                //     System.out.println(tweet.get("hashtags"));
                // }
                // System.out.println((new JsonParser().parse(jsonText.toString())).isJsonArray());
            }
            return true;
        }

        @Override
        public void close() throws IOException {
            if ( lineReader != null )
                lineReader.close();
        }
    }
}