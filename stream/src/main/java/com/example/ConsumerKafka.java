package com.example;


import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.functions.*;



public class ConsumerKafka {

        public static String sentimentAnalysis(int neg_rev_word_count, int pos_rev_word_count) {
                if (neg_rev_word_count - pos_rev_word_count > 50)
                    return "Negative";
                else if (neg_rev_word_count - pos_rev_word_count < -50)
                    return "Positive";
                else
                    return "Neutral";
            }
        
        

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: Review Sentiment Analysis <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("Sentiment Analysis")
                .master("local[*]")  // Set the master URL for local mode 
                .config("spark.sql.streaming.checkpointLocation", "checkpoint/")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from Kafka
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING())
                .selectExpr("split(value, ',') as data")
                .selectExpr("explode(split(data[8], ';')) as pneg_rev", 
                              "cast(data[0] as string) as track_name",
                              "cast(data[8] as int) as streams")
                        ;
     
        // Start running the query that prints the artist with max streams and the corresponding track name
        StreamingQuery query = df.writeStream()
                .outputMode("append")
                .format("console")
                .trigger(Trigger.ProcessingTime("20 second"))
                .start();

        query.awaitTermination();
    }
}



