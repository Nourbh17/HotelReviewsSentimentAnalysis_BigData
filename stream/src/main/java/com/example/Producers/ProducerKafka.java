package com.example.Producers;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ProducerKafka {

    public static String sentimentAnalysis(int neg_rev_word_count, int pos_rev_word_count) {
        if (neg_rev_word_count - pos_rev_word_count > 50)
            return "Negative";
        else if (neg_rev_word_count - pos_rev_word_count < -50)
            return "Positive";
        else
            return "Neutral";
    }



    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.out.println("Entrez le nom du topic Kafka en argument.");
            return;
        }

        String topicName = args[0];

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");

        props.put("acks", "all");

        props.put("retries", 0);

        props.put("batch.size", 16384);

        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

         // Path to the Excel file
        String excelFilePath = args[1];

        try (FileInputStream inputStream = new FileInputStream(excelFilePath)) {

            // Open the Excel workbook
            // Workbook workbook = WorkbookFactory.create(inputStream);

            Workbook workbook = new XSSFWorkbook(inputStream);
            // Select the first sheet of the workbook
            Sheet sheet = workbook.getSheetAt(0);

            // Iterate over the rows of the sheet
            for (Row row : sheet) {
                StringBuilder message = new StringBuilder();

                // Read the data from each cell of the row and concatenate
            
                for (Cell cell : row) {
                    if (message.length() > 0) {
                        message.append(", "); // Add a comma and a space between the values
                    }
                    String cellValue = cell.toString();
                    message.append(cellValue);
                }

                // traitement 
                String msg = message.toString();
                // String[] tab = msg.split(",");
                // int neg = (int) Double.parseDouble(tab[8]);;
                // int pos = (int) Double.parseDouble(tab[11]);; 
                // String sent = sentimentAnalysis(neg,pos);
                // String final_msg =tab[5] + tab[6] + sent  ;
                // // Send the message to the Kafka topic
                producer.send(new ProducerRecord<>(topicName, msg));

                System.out.println("Message sent successfully: " + msg);

            }

            // Close the Kafka producer
            producer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
