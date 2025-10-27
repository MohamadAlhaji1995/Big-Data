package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class App {
    public static void main(String[] args) {
        // SparkSession erstellen
        SparkSession spark = SparkSession.builder()
                .appName("JSON Join and Group Example")
                .master("local[*]")
                .getOrCreate();

        // JSON-Dateien aus HDFS lesen
        Dataset<Row> customers = spark.read().json("hdfs://localhost:9000/input/customers.json");
        Dataset<Row> orders = spark.read().json("hdfs://localhost:9000/input/orders.json");
        Dataset<Row> products = spark.read().json("hdfs://localhost:9000/input/products.json");

        // Join 1: Kunden + Bestellungen
        Dataset<Row> customerOrders = customers.join(orders, "customer_id");

        // Join 2: Ergebnis + Produkte
        Dataset<Row> fullJoin = customerOrders.join(products, "order_id");

        // Produkte pro Bestellung sammeln
        Dataset<Row> grouped = fullJoin
                .groupBy("order_id", "customer_id", "name", "total")
                .agg(collect_list("product").alias("products"));

        // Array in CSV-String umwandeln
        Dataset<Row> finalResult = grouped
                .withColumn("product", concat_ws(",", col("products")))
                .drop("products");

        // Ergebnis anzeigen
        finalResult.show(false);

        finalResult.coalesce(1).write().json("hdfs://localhost:9000/user/Mohamad/output/result");

        // Beenden
        spark.stop();
    }
}
