
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Github {

	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: Github <input> <output>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Github");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> github_data = context.textFile(args[0]);
		
		/**
		 * This generates repository data from github data that was read as csv file.
		 * Using the PairFunction, this creates a RDD that is mapped in following fashion. 
		 * Key = Language, Values = String(repository,stars)
		 */
		JavaPairRDD<String,String> repository_data = github_data.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] tokens = s.split(",");
				String repository = tokens[0];
				String language = tokens[1];
				String stars = tokens[12];
				// Integer stars = new Integer(tokens[12]);
				
				String key = language;
				String value = repository+","+stars;
				return new Tuple2<String, String>(key, value);
			}
		});
		
		/**
		 * This groups the repository data by the language used in it.
		 */
		JavaPairRDD<String, Iterable<String>> grouped_repositories = repository_data.groupByKey();
		
		grouped_repositories.saveAsTextFile(args[1]);
		context.stop();
		context.close();
	}
}