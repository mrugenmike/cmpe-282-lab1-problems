package com.wordcounter.mapper;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Stage1WordAppearanceMapper extends Mapper<LongWritable,Text,Text,Text> {
    final StopWords stopWordService = new StopWords();

    @Override
    protected void map(LongWritable offset,Text line,Context context) throws IOException, InterruptedException {
        // Compile all the words using regex
        Pattern p = Pattern.compile("\\w+");
        String cleanLine = line.toString().trim().toLowerCase().replaceAll("[^A-Za-z ]", "");

        Matcher m = p.matcher(cleanLine);
        String word1 = context.getConfiguration().get("word1").toLowerCase().trim();
        String word2 = context.getConfiguration().get("word2").toLowerCase().trim();

        // Get the name of the file from the inputsplit in the context
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        // build the values and write <k,v> pairs through the context
        StringBuilder valueBuilder = new StringBuilder();
        while (m.find()) {
            String word = m.group().toLowerCase().trim();
            // using stopword service instead of Stemmer algorithm
            if(!stopWordService.isStopWord(word)){
                // remove special characters and digits
                if (!Character.isLetter(word.charAt(0))
                        || Character.isDigit(word.charAt(0))) {
                    continue;
                }

                // emit the partial <k,v>
                if(word.equals(word1)||word.equals(word2)){
                    valueBuilder.append(fileName);
                    valueBuilder.append("@");
                    valueBuilder.append(offset.get());
                    context.write(new Text(valueBuilder.toString()),new Text(word));
                    valueBuilder.setLength(0);
                }

            }
        }
    }

}
