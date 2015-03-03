package com.tfidf.stage1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * WordFrequenceInDocument Creates the index of the words in documents,
 * mapping each of them to their frequency.
 * <p/>
 * Used Hadoop 0.20.2+737
 *
 *
 */
public class WordFrequenceInDocMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


        final StopWords stopWords = new StopWords();
        private static final Pattern PATTERN = Pattern.compile("\\w+");
        private Text word = new Text();
        private IntWritable singleCount = new IntWritable(1);

        public WordFrequenceInDocMapper() {
        }

        /**
         * POST-CONDITION: Output <"word", "filename@offset"> pairs
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Compile all the words using regex

            String cleanLine = value.toString().trim().toLowerCase().replaceAll("[^A-Za-z ]+", "");
            Matcher m = PATTERN.matcher(cleanLine);

            // Get the name of the file from the input-split in the context
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            // build the values and write <k,v> pairs through the context
            StringBuilder valueBuilder = new StringBuilder();
            while (m.find()) {
                String matchedKey = m.group().toLowerCase();
                // remove names starting with non letters, digits, considered stopwords or containing other chars
                if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))
                        || stopWords.isStopWord(matchedKey) || matchedKey.contains("_") ||
                        matchedKey.length() < 3) {
                    continue;
                }
                valueBuilder.append(matchedKey);
                valueBuilder.append("@");
                valueBuilder.append(fileName);
                // emit the partial <k,v>
                this.word.set(valueBuilder.toString());
                context.write(this.word, this.singleCount);
                valueBuilder.setLength(0);
            }
        }
    }




