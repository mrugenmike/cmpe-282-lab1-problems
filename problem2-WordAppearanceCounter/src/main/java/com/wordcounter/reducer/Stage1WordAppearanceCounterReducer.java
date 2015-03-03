package com.wordcounter.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Stage1WordAppearanceCounterReducer extends Reducer<Text,IntWritable,Text, IntWritable> {
    @Override
    protected void reduce(Text wordsFound, Iterable<IntWritable> wordsOnTheLine, Context context)
            throws IOException, InterruptedException {
        String wordOfInterest1 = context.getConfiguration().get("word1").toLowerCase().trim();
        String wordOfInterest2 = context.getConfiguration().get("word2").toLowerCase().trim();
        int totalCount = 0 ;
        for(IntWritable wordCount: wordsOnTheLine){
            totalCount+=wordCount.get();
        }
        context.write(new Text(String.format("%s and %s  appears on same line for following times: ",wordOfInterest1,wordOfInterest2)),new IntWritable(totalCount));

    }
}


