package com.tfidf.stage1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by mrugen on 3/2/15.
 */
public class WordFrequenceInDocReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable wordSum = new IntWritable();

    public WordFrequenceInDocReducer() {
    }

    /**
     * @param key     is the key of the mapper
     * @param values  are all the values aggregated during the mapping phase
     * @param context contains the context of the job run
     *                <p/>
     *                PRE-CONDITION: receive a list of <"word@filename",[1, 1, 1, ...]> pairs
     *                <"marcello@a.txt", [1, 1]>
     *                <p/>
     *                POST-CONDITION: emit the output a single key-value where the sum of the occurrences.
     *                <"marcello@a.txt", 2>
     */
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        //write the key and the adjusted value (removing the last comma)
        this.wordSum.set(sum);
        context.write(key, this.wordSum);
    }
}