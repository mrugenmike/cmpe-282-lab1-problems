package com.wordcounter.mapper;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Stage1WordAppearanceMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable offset,Text line,Context context) throws IOException, InterruptedException {
        String cleanLine = line.toString().trim().toLowerCase().replaceAll("[^A-Za-z ]+", "");

        String wordOfInterest = context.getConfiguration().get("word1").toLowerCase().trim();
        String wordOfInterest2 = context.getConfiguration().get("word2").toLowerCase().trim();

        String regex = String.format("%s.*%s|%s.*%s", wordOfInterest, wordOfInterest2, wordOfInterest2, wordOfInterest);
        Pattern pattern1 = Pattern.compile(regex);

        Matcher matcher1 = pattern1.matcher(cleanLine);

        if(matcher1.find() ){
            context.write(new Text(wordOfInterest + " " + wordOfInterest2), new IntWritable(1));
        }
    }

}
