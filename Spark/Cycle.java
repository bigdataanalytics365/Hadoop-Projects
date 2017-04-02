import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Cycle {

	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: Cycle <input> <output>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Cycle");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> patent_input = context.textFile(args[0]);
		
		/**
		 * This maps the vertices in for each edge as they are read from the file in following format.
		 * Key = From vertex, Values = Edge(fromVertex, toVertex) as a String
		 */
		JavaPairRDD<String,String> patents = patent_input.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] tokens = s.split("\\s+");
				String fromVertex = tokens[0];
				String toVertex = tokens[1];
				
				String key = fromVertex;
				String value = fromVertex+","+toVertex;
				return new Tuple2<String, String>(key, value);
			}
		});
			
		results.saveAsTextFile(args[1]);
		context.stop();
		context.close();
	}
}