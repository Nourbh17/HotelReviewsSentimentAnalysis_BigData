package com.example;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;



public class ConsumerKafka {

     
        
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
                .config("spark.sql.streaming.checkpointLocation", "/checkpoint/")
                .getOrCreate();
       
       

        // Register UDF
        UDFRegistration udf = spark.udf();
        udf.register("sentimentAnalysis", (UDF2<Integer, Integer, String>) 
                (neg_rev_word_count, pos_rev_word_count) -> {
                    if (neg_rev_word_count - pos_rev_word_count >= 50 || pos_rev_word_count == 0  )
                        return "Negative";
                    else if (neg_rev_word_count - pos_rev_word_count <= -50 || neg_rev_word_count == 0)
                        return "Positive";
                    else
                        return "Neutral";
                }, DataTypes.StringType);

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
                .selectExpr("cast(data[5] as string) as Hotel_Name",
                              "cast(data[6] as string) as Reviewr_Nationality",
                              "cast(data[13] as string) as Day_since_review",
                              "cast(data[8] as int) as neg_rev",
                              "cast(data[11] as int) as pos_rev");
         
        Dataset<Row> dfWithSentiment = df.withColumn("sentiment",
                callUDF("sentimentAnalysis", col("neg_rev"), col("pos_rev")));


        String collectionName = "HotelsReviews";

    StreamingQuery query = dfWithSentiment.writeStream()
                .outputMode("append")
                .trigger(Trigger.ProcessingTime("20 seconds"))
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDF, batchId) -> {
        // Process each batch here
                    batchDF.persist(); // Cache the DataFrame for better performance if needed

        // Example processing: Print the content of the batch DataFrame
                    System.out.println("Batch ID: " + batchId);
                    batchDF.show(false); // Show the content of the batch DataFrame
        
        
                    

        // Example processing: Write the batch data to MongoDB
                batchDF.write()
                    .format("com.mongodb.spark.sql.DefaultSource")
                    .mode("append")
                    .option("spark.mongodb.output.uri",  "mongodb+srv://benhajlanour2:gPUbF29WMwsl9j4y@cluster0.blvpf1h.mongodb.net/Hotels." + collectionName)
                    .save();
        
        // Unpersist the DataFrame to release memory
        batchDF.unpersist();
    })
    .start();

    query.awaitTermination();


    }
}



