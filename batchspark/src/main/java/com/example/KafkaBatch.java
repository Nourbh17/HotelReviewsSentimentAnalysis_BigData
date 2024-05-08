package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class KafkaBatch extends Configured implements Tool {



    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream outputStream;

        if (fs.exists(new Path(args[3]))) {
            outputStream = fs.append(new Path(args[3]));
           
        } else {
            outputStream = fs.create(new Path(args[3]));
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, args[1]);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(args[2]));

        // System.out.println("hellooooooooooooooo!!!!!");

        Job job = Job.getInstance(conf, "Kafka Batch MapReduce");
        job.setJarByClass(KafkaBatch.class);
        job.setMapperClass(CountryTokenizeMapper.class);
        job.setReducerClass(HotelCountReducer.class);    
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // String path = args[3] + ".txt";
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        try {
            int i=0;
                while(i <= 3){

               
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    outputStream.writeBytes(record.value() + "\n");
                
                outputStream.hsync();
                consumer.commitAsync();
            }
         i++;
        } 
    }catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            fs.close();
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new KafkaBatch(), args);
        if (exitCode == 0 ){
             
            // System.out.println("hereeeeeeeeeee mongoooo ");
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(args[4]));
            for (int i = 0; i < status.length; i++) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                String uri = "mongodb+srv://benhajlanour2:gPUbF29WMwsl9j4y@cluster0.blvpf1h.mongodb.net/";
                ConnectionString mongoURI = new ConnectionString(uri);
                MongoClientSettings settings = MongoClientSettings.builder()
                        .applyConnectionString(mongoURI)
                        .build();
                MongoClient mongoClient = MongoClients.create(settings);
                MongoDatabase database = mongoClient.getDatabase("Hotels");
                MongoCollection<Document> collection = database.getCollection("batch");

                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\t");
                    String[] countryStars = fields[0].split(",");

                    Document doc = new Document("country", countryStars[0])
                            .append("stars", Integer.parseInt(countryStars[1]))
                            .append("hotels", Integer.parseInt(fields[1]));
                    
                   collection.insertOne(doc);
                }

                mongoClient.close();
                
            }

            System.exit(0);
        } else {
            // System.out.println("failed mongoooo ");
            System.exit(1);
        }
        System.exit(1);
    }
}