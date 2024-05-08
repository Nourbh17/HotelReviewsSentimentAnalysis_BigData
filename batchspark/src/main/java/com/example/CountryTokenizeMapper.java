package com.example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountryTokenizeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text countryStars = new Text();

public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
                
        String line = value.toString();
        if(!line.startsWith("countyName")){
            String[] fields = value.toString().split(",");
            if(fields.length>=5){
                String country = fields[0];
                String star = fields[4];
                countryStars.set(country + "," + star);
                context.write(countryStars, one);
                }
        }
    }
}