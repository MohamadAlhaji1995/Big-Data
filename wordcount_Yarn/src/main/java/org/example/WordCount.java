import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: WordCount <input> <output>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(args[0]);
        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<>(w, 1)).reduceByKey(Integer::sum);
        counts.saveAsTextFile(args[1]);

        sc.close();
    }
}
