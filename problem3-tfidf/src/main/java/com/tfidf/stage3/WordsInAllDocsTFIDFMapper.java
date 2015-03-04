package com.tfidf.stage3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by mrugen on 3/2/15.
 */
public class WordsInAllDocsTFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text wordAndDoc = new Text();
    private Text wordAndCounters = new Text();

    /**
     *     INPUT: marcello@book.txt    3/1500
     *     OUTPUT: marcello, book.txt=3/1500,1
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndCounters = value.toString().split("\t");
        String[] wordAndDoc = wordAndCounters[0].split("@");  //3/1500
        this.wordAndDoc.set(new Text(wordAndDoc[0]));
        this.wordAndCounters.set(wordAndDoc[1] + "=" + wordAndCounters[1]);
        context.write(this.wordAndDoc, this.wordAndCounters);
    }
}
