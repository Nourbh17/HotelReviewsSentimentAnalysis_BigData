package com.example;


import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import static org.apache.spark.sql.functions.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;




public class ConsumerKafka {

        // public static String sentimentAnalysis(int neg_rev_word_count, int pos_rev_word_count) {
        //         if (neg_rev_word_count - pos_rev_word_count > 50)
        //             return "Negative";
        //         else if (neg_rev_word_count - pos_rev_word_count < -50)
        //             return "Positive";
        //         else
        //             return "Neutral";
        //     }
        
        

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
                    if (neg_rev_word_count - pos_rev_word_count > 50)
                        return "Negative";
                    else if (neg_rev_word_count - pos_rev_word_count < -50)
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
            // .selectExpr("explode(split(data[5], ';')) as neg_rev", 
                .selectExpr("cast(data[5] as string) as Hotel_Name",
                              "cast(data[8] as int) as neg_rev",
                              "cast(data[11] as int) as pos_rev");
            // .map((MapFunction) line -> {

            // });
        Dataset<Row> dfWithSentiment = df.withColumn("sentiment",
                callUDF("sentimentAnalysis", col("neg_rev"), col("pos_rev")));

        // dfWithSentiment.show();



        String mongoConnectionString  = "mongodb://localhost:27017";
        String mongoDatabaseName = "your-database-name";
        String mongoCollectionName = "your-collection-name";


        MongoClient mongoClient = MongoClients.create(mongoConnectionString);
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);



        // ConnectionString mongoURI = new ConnectionString(uri);
        //             MongoClientSettings settings = MongoClientSettings.builder()
        //                     .applyConnectionString(mongoURI)
        //                     .build();
        // MongoClient mongoClient = MongoClients.create(settings);
        // MongoDatabase database = mongoClient.getDatabase("GlobalTerrorism");
        // MongoCollection<Document> collection = database.getCollection("stream");
        // Document doc = new Document("pairs", );
        // collection.insertOne(doc);
        // mongoClient.close();
       

     
        // Start running the query that prints the artist with max streams and the corresponding track name
        StreamingQuery query = dfWithSentiment.writeStream()
                .outputMode("append")
                .format("console")
                // .foreachBatch((batchDF, batchId) -> {
                //     // Write each batch to MongoDB
                //     batchDF.write()
                //             .format("mongodb")
                //             //.option("checkpointLocation", "/tmp/")
                //             //.option("forceDeleteTempCheckpointLocation", "true")
                //             //.option("spark.mongodb.connection.uri", "mongodb://localhost:27017/")
                //             .option("database", "BigData_Project")
                //             .option("collection", "stream")
                //             .mode("append")
                //             .save();
                // })
                .trigger(Trigger.ProcessingTime("20 second"))
                .start();

        query.awaitTermination();
    }
}



