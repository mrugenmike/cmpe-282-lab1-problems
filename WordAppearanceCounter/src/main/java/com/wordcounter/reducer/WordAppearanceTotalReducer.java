package com.wordcounter.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordAppearanceTotalReducer extends Reducer<Text,Text,Text,NullWritable> {
    @Override
    protected void reduce(Text lineWithWordsOfInterest,Iterable<Text> values,Context context) throws IOException, InterruptedException {
        int totalOccurence = 0;
        String word1 = context.getConfiguration().get("word1").toLowerCase();
        String word2 = context.getConfiguration().get("word2").toLowerCase();

        for(Text value:values){
            totalOccurence+=Integer.valueOf(value.toString());
        }
        context.write(new Text(String.format("%s  appears %d times with %s",word1,totalOccurence,word2)),NullWritable.get());
    }
}
