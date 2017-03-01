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
public class TweetAnalysis {
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // String input = "/class/s17419/lab5/university.json";
        // String hashTagOutput = "/scr/rvshah/lab5/exp1/output/";
        // String followerOutput = "/scr/rvshah/lab5/exp2/output/";
        // String profilicOutput = "/scr/rvshah/lab5/exp3/output/";

        String input = "/home/rushabhs/Desktop/CPRE/419/lab5/test.json";
        String hashTagOutput = "/home/rushabhs/Desktop/CPRE/419/lab5/outputHashtag";
        String followerOutput = "/home/rushabhs/Desktop/CPRE/419/lab5/outputFollower";
        String profilicOutput = "/home/rushabhs/Desktop/CPRE/419/lab5/outputProfilic";
        String mostProfilicOutput = "/home/rushabhs/Desktop/CPRE/419/lab5/outputMostProfilic";
        
        int reduce_tasks = 1;
        
        // Create a Hadoop Job
        Job hashtagJob = Job.getInstance(conf, "Hashtags Job");
        
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
        FileOutputFormat.setOutputPath(hashtagJob, new Path(hashTagOutput));
        
        // Run the job
        hashtagJob.waitForCompletion(true);
        
        // Create a Hadoop Job
        Job followerJob = Job.getInstance(conf, "Follower Job");
        
        // Attach the job to this Class
        followerJob.setJarByClass(TweetAnalysis.class); 
        
        // Number of reducers
        followerJob.setNumReduceTasks(reduce_tasks);
        
        followerJob.setMapOutputKeyClass(Text.class);
        followerJob.setMapOutputValueClass(IntWritable.class);
        followerJob.setOutputKeyClass(Text.class); 
        followerJob.setOutputValueClass(Text.class);
        
        // Set the Map class
        followerJob.setMapperClass(FolloweMapper.class); 
        followerJob.setReducerClass(FollowerReducer.class);
        
        // Set how the input is split
        // TextInputFormat.class splits the data per line
        followerJob.setInputFormatClass(JSONInputFormat.class); 
        
        // Output format class
        followerJob.setOutputFormatClass(TextOutputFormat.class);
        
        // Input path
        FileInputFormat.addInputPath(followerJob, new Path(input)); 
        
        // Output path
        FileOutputFormat.setOutputPath(followerJob, new Path(followerOutput));
        
        // Run the job
        followerJob.waitForCompletion(true);

        // Create a Hadoop Job
        Job profilicUserJob = Job.getInstance(conf, "Profilic User Job");
        
        // Attach the job to this Class
        profilicUserJob.setJarByClass(TweetAnalysis.class); 
        
        // Number of reducers
        profilicUserJob.setNumReduceTasks(reduce_tasks);
        
        profilicUserJob.setMapOutputKeyClass(Text.class);
        profilicUserJob.setMapOutputValueClass(Text.class);
        profilicUserJob.setOutputKeyClass(Text.class); 
        profilicUserJob.setOutputValueClass(IntWritable.class);
        
        // Set the Map class
        profilicUserJob.setMapperClass(ProfilicUserMapper.class); 
        profilicUserJob.setReducerClass(ProfilicUserReducer.class);
        
        // Set how the input is split
        // TextInputFormat.class splits the data per line
        profilicUserJob.setInputFormatClass(JSONInputFormat.class); 
        
        // Output format class
        profilicUserJob.setOutputFormatClass(TextOutputFormat.class);
        
        // Input path
        FileInputFormat.addInputPath(profilicUserJob, new Path(input)); 
        
        // Output path
        FileOutputFormat.setOutputPath(profilicUserJob, new Path(profilicOutput));
        
        // Run the job
        profilicUserJob.waitForCompletion(true);

        // Create a Hadoop Job
        Job profilicJob = Job.getInstance(conf, "Profilic Job");
        
        // Attach the job to this Class
        profilicJob.setJarByClass(TweetAnalysis.class); 
        
        // Number of reducers
        profilicJob.setNumReduceTasks(reduce_tasks);
        
        profilicJob.setMapOutputKeyClass(Text.class);
        profilicJob.setMapOutputValueClass(IntWritable.class);
        profilicJob.setOutputKeyClass(Text.class); 
        profilicJob.setOutputValueClass(IntWritable.class);
        
        // Set the Map class
        profilicJob.setMapperClass(ProfilicMapper.class); 
        profilicJob.setReducerClass(ProfilicReducer.class);
        
        // Set how the input is split
        // TextInputFormat.class splits the data per line
        profilicJob.setInputFormatClass(TextInputFormat.class); 
        
        // Output format class
        profilicJob.setOutputFormatClass(TextOutputFormat.class);
        
        // Input path
        FileInputFormat.addInputPath(profilicJob, new Path(profilicOutput)); 
        
        // Output path
        FileOutputFormat.setOutputPath(profilicJob, new Path(mostProfilicOutput));
        
        // Run the job
        profilicJob.waitForCompletion(true);
    } 

    /**
	  * HashtagMapper class
	  * This class will map the hashtag text to int 1.
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
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
            int count = 0;
            for (IntWritable val : values) {
                count++;
            }
            context.write(key, new Text(Integer.toString(count)));
        }
    }

    /**
	  * FolloweMapper class
	  * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	  * 
	  * Input:
	  *		KEY: File offset
	  *		VALUE: The JSON string read as custom input.
	  * Output:
	  *		KEY: Screen_name from the 'user' JsonObject.
	  *		VALUE: Followers_count from the 'user' JsonObject
	  */
    public static class FolloweMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
            JsonParser parser = new JsonParser();
            JsonObject user = parser.parse(value.toString()).getAsJsonObject().getAsJsonObject("user");
                // Emit the screen_name => followers_count
                context.write(new Text(user.getAsJsonPrimitive("screen_name").getAsString()), new IntWritable(user.getAsJsonPrimitive("followers_count").getAsInt()));
        }
    }
    
    /**
	  * FollowerReducerReduce class
	  * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	  * 
	  * Input:
	  *		KEY: Screen_name
	  *		VALUE: followers_count list of this screen_name
	  * Output:
	  *		KEY: Screen_name
	  *		VALUE: Max followers_count from the list of followers_count.
	  */
    public static class FollowerReducer extends Reducer<Text, IntWritable, Text, Text>  {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
            int maxFollower = 0;
            for (IntWritable val : values) {
                // Compute the max followers_count for this user.
                if(val.get() > maxFollower){
                    maxFollower = val.get();
                }
            }
            context.write(key, new Text(Integer.toString(maxFollower)));
        }
    }

    /**
	  * ProfilicMapper class
	  * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	  * 
	  * Input:
	  *		KEY: File offset
	  *		VALUE: The JSON string read as custom input.
	  * Output:
	  *		KEY: Screen_name from the 'user' object
	  *		VALUE: 1
	  */
    public static class ProfilicUserMapper extends Mapper<LongWritable, Text, Text, Text>  {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
            JsonParser parser = new JsonParser();
            JsonObject tweet = parser.parse(value.toString()).getAsJsonObject();
            // Get the screen name of this user
            String user = tweet.getAsJsonObject("user").getAsJsonPrimitive("screen_name").getAsString();
            // Get all hashtags from this tweet.
            JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
            StringBuilder hts = new StringBuilder();
            if(hashtags != null && hashtags.size() > 0){
                for(JsonElement tag: hashtags){
                    JsonObject temp = tag.getAsJsonObject();
                    String tagText = temp.get("text").getAsString();
                    // Separate each hashtag by # sign.
                    if(tagText != null && !tagText.equals("")){
                        hts.append("#" + tagText);
                    }
                }
                // Add a colon to separate different tweets.
                hts.append(";");
                context.write(new Text(user), new Text(hts.toString()));
            }
        }
    }
    
    /**
	  * ProfilicReducer class
	  * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	  * 
	  * Input:
	  *		KEY: hashtags
	  *		VALUE: 1
	  * Output:
	  *		KEY: hashtags
	  *		VALUE: # of times 
	  */
    public static class ProfilicUserReducer extends Reducer<Text, Text, Text, IntWritable>  {
        // Maps users to number of tweets.
        Map<Text, Integer> users = new HashMap<Text, Integer>();
        Map<Text, Text> popHashtag = new HashMap<Text, Text>();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
            String screen_name =  key.toString();
            for (Text val : values) {
                // Split the tags by ';', which denotes they belog to separate tweets.
                String[] hashTags = val.toString().split(";");
                // hashTags.length is the number of tweets by user 'key'
                users.put(key, hashTags.length);
                popHashtag.put(key, val);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int count = 0;
            List<Map.Entry<Text, Integer>> userList = new LinkedList<Map.Entry<Text, Integer>>(users.entrySet());
            // Sort the list in decending fashion for the number of tweets.
            Collections.sort(userList, new Comparator<Map.Entry<Text, Integer>>() {
            public int compare( Map.Entry<Text, Integer> o1, Map.Entry<Text, Integer> o2 ) {
                return (o2.getValue()).compareTo(o1.getValue() );
            }
            });
            for(int i=0; i<10; i++){
                Map.Entry<Text, Integer> user = userList.get(i);
                String[] allHt = popHashtag.get(user.getKey()).toString().split("#");
                for(String tag: allHt){
                    if(!tag.isEmpty() && tag != null){
                        // Emit all tags with 1 for these top prolific users.
                        context.write(new Text(tag), new IntWritable(1));
                    }
                }
            }
        }
    }

    /**
	  * ProfilicMapper class
	  * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	  * 
	  * Input:
	  *		KEY: File offset
	  *		VALUE: The JSON string read as custom input.
	  * Output:
	  *		KEY: Hashtag name
	  *		VALUE: value 1
	  */
    public static class ProfilicMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
            // Just emit it as it is. We will aggregate it in reducer.
            context.write(value, new IntWritable(1));
        }
    }
    
    /**
	  * ProfilicReducer class
	  * This class will map convert the file to SequenceFileOutputFormat to create partitions.
	  * 
	  * Input:
	  *		KEY: hashtags
	  *		VALUE: 1
	  * Output:
	  *		KEY: hashtags
	  *		VALUE: # of times 
	  */
    public static class ProfilicReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
            int count = 0;
            for (IntWritable val : values) {
                count++;
            }
            // Hashtag and total count among the top prolific users.
            context.write(key, new IntWritable(count));
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