package org.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class IPVisitedPagesCSV {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: IPVisitedPagesCSV <input_csv> <output_path>");
            System.exit(1);
        }

        String inputCsv = args[0];
        String outputPath = args[1];

        SparkSession spark = SparkSession.builder()
            .appName("IP to Visited Pages from CSV")
            .getOrCreate();

        Dataset<Row> logs = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(inputCsv);

        Dataset<Row> result = logs
            .groupBy("ip")
            .agg(functions.collect_set("url").as("visited_pages"));

        result.write().mode("overwrite").json(outputPath);

        spark.stop();
    }
}
