package com.wordcounter.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordAppearanceCounterMapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable line,Text words,Context context) throws IOException, InterruptedException {
        System.out.println("******MAP********");
        context.write(new Text("hello"),new Text("1"));
    }
}
