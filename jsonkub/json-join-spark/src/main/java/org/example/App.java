package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class App {
    public static void main(String[] args) {
        System.out.println(">>> Starte JSON Join...");

        SparkSession spark = SparkSession.builder()
            .appName("JSON Join Spark with MinIO")
            .getOrCreate();

        System.out.println(">>> Spark Master: " + spark.sparkContext().master());

        try {
            System.out.println(">>> Lese customers.json");
            Dataset<Row> customers = spark.read().json("s3a://mybucket/customers.json");
            customers.show();

            System.out.println(">>> Lese orders.json");
            Dataset<Row> orders = spark.read().json("s3a://mybucket/orders.json");
            orders.show();

            System.out.println(">>> Lese products.json");
            Dataset<Row> products = spark.read().json("s3a://mybucket/products.json");
            products.show();

            System.out.println(">>> Joine Tabellen");
            Dataset<Row> joined = orders
                .join(customers, "customer_id")
                .join(products, "order_id");

            Dataset<Row> grouped = joined
                .groupBy(
                    col("order_id"),
                    col("customer_id"),
                    col("name"),
                    col("total")
                )
                .agg(collect_list("product").as("products"))
                .withColumn("product", concat_ws(",", col("products")))
                .drop("products");

            grouped.show(false);

            String outputPath = "s3a://mybucket/output/result";

            System.out.println(">>> Lösche vorherige Ausgabe falls vorhanden: " + outputPath);
            Path out = new Path(outputPath);
            FileSystem fs = out.getFileSystem(spark.sparkContext().hadoopConfiguration());
            if (fs.exists(out)) {
                fs.delete(out, true);
                System.out.println(">>> Vorherige Ausgabe gelöscht");
            }

            System.out.println(">>> Schreibe Ausgabe nach S3");
            grouped.write()
                .mode("overwrite")
                .json(outputPath);

            System.out.println(">>> Fertig.");

        } catch (Exception e) {
            e.printStackTrace();
        }

        spark.stop();
    }
}
