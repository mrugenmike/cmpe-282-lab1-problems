package com.wordcounter.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Stage1WordAppearanceCounterReducer extends Reducer<Text,Text,Text, Text> {
    @Override
    protected void reduce(Text lineWithOffset, Iterable<Text> wordsOnTheLine, Context context)
            throws IOException, InterruptedException {
        String word1 = context.getConfiguration().get("word1").toLowerCase().trim();
        String word2 = context.getConfiguration().get("word2").toLowerCase().trim();


        Set<String> wordsOfInterest = new HashSet<String>();

        for (Text word : wordsOnTheLine) {
            if (word.toString().equals(word1) || word.toString().equals(word2)) {
                wordsOfInterest.add(word.toString());
            }
        }

        if (wordsOfInterest.contains(word1) && wordsOfInterest.contains(word2)) {
            context.write(new Text(lineWithOffset), new Text(word1 + "," + word2));
        }
    }
}


