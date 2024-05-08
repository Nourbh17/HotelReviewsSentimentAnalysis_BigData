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
            int batchSize = 250;
            int rowCount = 0;


            // Iterate over the rows of the sheet
            for (Row row : sheet) {
                StringBuilder message = new StringBuilder();

                // Read the data from each cell of the row and concatenate
            
                for (Cell cell : row) {
                    if (message.length() > 0) {
                        message.append(","); // Add a comma and a space between the values
                    }
                    String cellValue = cell.toString();
                    message.append(cellValue);
                }

                // traitement 
                String msg = message.toString();
                producer.send(new ProducerRecord<>(topicName, msg));
                
                rowCount++;

                if (rowCount % batchSize == 0) {
                    Thread.sleep(10000); // Sleep for 10 second after sending a batch
                }

                System.out.println("Message sent successfully: " + msg);

            }

            // Close the Kafka producer
            producer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
