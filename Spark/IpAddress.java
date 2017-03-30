
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class IpAddress {

	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.err.println("Usage: IpAddress <input> <output>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("IpAddress");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> trace_file = context.textFile(args[0]);
		JavaRDD<String> raw_block_file = context.textFile(args[1]);
		
		JavaPairRDD<String,String> trace = trace_file.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] tokens = s.split("\\s+");
				String time = tokens[0];
				String cid = tokens[1];
				String source = tokens[2];
				String destination = tokens[4];
				
				String key = cid;
				String value = time+","+cid+","+source+","+destination;
				return new Tuple2<String, String>(key, value);
			}
		});
		
		JavaPairRDD<String,String> raw_block = raw_block_file.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] tokens = s.split("\\s+");
				String cid = tokens[0];
				String status = tokens[1];
				
				String key = cid;
				String value = status;
				return new Tuple2<String, String>(key, value);
			}
		});
		
		JavaPairRDD<String, String> blocked = raw_block.filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				String value = tuple._2;
				if (value.equals("Blocked")) {
					return true;
				}
				return false;
			}
		});

		JavaPairRDD<String, Tuple2<String, String>> firewall_blocked = trace.join(blocked, numOfReducers);

		JavaRDD<String> firewall = firewall_blocked.map(new Function<Tuple2<String, Tuple2<String, String>>, String>() {
			@Override
			public String call(Tuple2<String, Tuple2<String, String>> item) throws Exception {
				String cid = item._1;
				Tuple2<String, String> logT = item._2;
				String log = logT._1 + "," + logT._2;
				log = log.replace(","," 		");
				return log;
			}
		});
		
		JavaPairRDD<String, String> sources = firewall_blocked.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> item) throws Exception {
				String cid = item._1;
				Tuple2<String, String> logT = item._2;
				String log = logT._1 + "," + logT._2;
				String[] fields = logT._1.split(",");
				String source = fields[2];
				return new Tuple2(source, log);
			}
		});
		
		// Map<String, Integer> blocked_sources = sources.countByKey();
		JavaPairRDD<String, Iterable<String>> group_sources = sources.groupByKey();
			
		JavaPairRDD<Integer, String> blocked_sources = group_sources.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Iterable<String>> item) throws Exception {
				String source = item._1;
				int count = 0;
				for (String s : item._2) {
					count++;
				}
				return new Tuple2(new Integer(count), source);
			}
		});
		
		JavaPairRDD<Integer, String> ordered_reversed = blocked_sources.sortByKey(false);
		
		JavaPairRDD<String, Integer> blocked_sources_counts = ordered_reversed.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
				return item.swap();
			}
		});
			
		firewall.saveAsTextFile(args[2]);
		blocked_sources_counts.saveAsTextFile(args[3]);
		context.stop();
		context.close();
	}
}