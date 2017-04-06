import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import java.util.*;

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
		 * Map the key value pair in order as they are read.
		 * Key = fromVertex, Values = toVertex
		 */
		JavaPairRDD<String,String> citations = patent_input.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] tokens = s.split("\\s+");
				String fromVertex = tokens[0];
				String toVertex = tokens[1];
				return new Tuple2<String, String>(fromVertex, toVertex+"TO");
			}
		});
		
		/**
		 * Map the key value pair in order as they are read.
		 * Key = toVertex, Values = fromVertex
		 */
		JavaPairRDD<String,String> citations_reversed = patent_input.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] tokens = s.split("\\s+");
				String fromVertex = tokens[0];
				String toVertex = tokens[1];
				return new Tuple2<String, String>(toVertex, fromVertex+"FROM");
			}
		});
		
		/**
		* This join gives the key it joined by with vertexFrom and vertexTo in the same tuple that was mapped above, forming the 'open triads'
		*/
		JavaPairRDD<String,Tuple2<String,String>> joined = citations.join(citations_reversed);
		
		/**
		 * This will extract out all the edges that will need to be present in the original graph which will make a one complete cycle of length 3.
		 */
		JavaPairRDD<String,String> edges_to_check = joined.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,String>>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String,Tuple2<String,String>> s) throws Exception {
				Tuple2<String,String> adj = s._2;
				String to = adj._1;
				String from = adj._2;
				// Added TO at the end so it matches with the first 'citation' map pair function during intersection.
				return new Tuple2<String,String>(to.substring(0,to.length()-2),from.substring(0,from.length()-4)+"TO");
			}
		});
		
		/**
		 * Intersect with the original citation RDD to see what edges are present to enumerate number of cycles with length 3.
		 * Number of cycles = # of records in matched_edges / 3.
		 */
		JavaPairRDD<String,String> matched_edges = citations.intersection(edges_to_check);
		
		matched_edges.saveAsTextFile(args[1]);
		// System.out.println(results.getClass().getName());
		context.stop();
		context.close();
	}
}