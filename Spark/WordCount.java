
import java.util.Arrays;
import java.util.Comparator;
import java.io.Serializable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

public class WordCount {

	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: WordCount <input> <output>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("WordCount in Spark");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = context.textFile(args[0]);

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(s.split("\\s+"));
			}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> unordered_counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numOfReducers);

		JavaPairRDD<Integer, String> swappedPair = unordered_counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
				return item.swap();
			}
	   });

		JavaPairRDD<Integer, String> ordered_reversed = swappedPair.sortByKey(false);

		JavaPairRDD<String, Integer> counts = ordered_reversed.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
				return item.swap();
			}
		});
		
		counts.saveAsTextFile(args[1]);
		context.stop();
		context.close();
		
	}
	
	private static class CountComparator implements Comparator<Tuple2<Integer, String>>, Serializable {
	    @Override
	    public int compare(Tuple2<Integer, String> tuple1, Tuple2<Integer, String> tuple2) {
	        return tuple1._1 - tuple2._1;
	    }
	}
}