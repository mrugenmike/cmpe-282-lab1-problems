package com.tfidf.stage2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mrugen on 3/2/15.
 */
public class WordCountsForDocsReducer extends Reducer<Text, Text, Text, Text> {

private Text wordAtDoc = new Text();
private Text wordAvar = new Text();

/**
 *
 *        Input: receive a list of <document, ["word=n", "word-b=x"]>
 *            pairs <"a.txt", ["word1=3", "word2=5", "word3=5"]>
 *
 *       POST-CONDITION: <"word1@a.txt, 3/13">,
 *            <"word2@a.txt, 5/13">
 */
protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumOfWordsInDocument = 0;
        Map<String, Integer> tempCounter = new HashMap<String, Integer>();
        for (Text val : values) {
        String[] wordCounter = val.toString().split("=");
        tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
        sumOfWordsInDocument += Integer.parseInt(val.toString().split("=")[1]);
        }
        for (String wordKey : tempCounter.keySet()) {
        this.wordAtDoc.set(wordKey + "@" + key.toString());
        this.wordAvar.set(tempCounter.get(wordKey) + "/" + sumOfWordsInDocument);
        context.write(this.wordAtDoc, this.wordAvar);
        }
        }
}
