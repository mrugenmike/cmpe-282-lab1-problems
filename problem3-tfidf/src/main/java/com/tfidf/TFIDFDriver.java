package com.tfidf;

import com.tfidf.stage1.WordFrequenceInDocMapper;
import com.tfidf.stage1.WordFrequenceInDocReducer;
import com.tfidf.stage2.WordCountsForDocsMapper;
import com.tfidf.stage2.WordCountsForDocsReducer;
import com.tfidf.stage3.WordsInAllDocsTFIDFMapper;
import com.tfidf.stage3.WordsInAllDocsTFIDReducer;
import com.tfidf.util.FileSplitter;
import com.tfidf.util.LocalToHDFSCopier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TFIDFDriver extends Configured implements Tool {
    private static final String OUTPUT_PATH_STAGE1 = "stage1op";
    private static final String OUTPUT_PATH_STAGE2 = "stage2op";

    @Override
    public int run(String[] args) throws Exception {
        if(args==null||args.length!=4){
            System.out.print("\n Please specify the input path and output path for the files");
            System.out.print("\n example: hadoop jar <your_jar> <inputPath> <outputPath> <word1> <word2> \n");
            return -1;
        }

        Configuration conf = getConf();
        conf.set("word1",args[2]);
        conf.set("word2",args[3]);

        FileSystem fs = FileSystem.get(conf);
        Path finalOutput = new Path(args[1]);
        deletePaths(args[1],fs);
        deletePaths(OUTPUT_PATH_STAGE1,fs);
        deletePaths(OUTPUT_PATH_STAGE2,fs);

        FileStatus[] userFilesStatusList = fs.listStatus(new Path(args[0]));
        final int numberOfUserInputFiles = userFilesStatusList.length;


        /**Stage 1************************************************/
        Job job = Job.getInstance(conf, "Word Frequency In Document");

        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(WordFrequenceInDocMapper.class);
        job.setReducerClass(WordFrequenceInDocReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_STAGE1));

        job.waitForCompletion(true);

        /**Stage 2************************************************/
        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2, "Words Counts");
        job2.setJarByClass(TFIDFDriver.class);
        job2.setMapperClass(WordCountsForDocsMapper.class);
        job2.setReducerClass(WordCountsForDocsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH_STAGE1));
        FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH_STAGE2));

        job2.waitForCompletion(true);

        /**Stage 3***********************************************/

        Configuration conf3 = getConf();
        conf3.setInt("numberOfDocsInCorpus", numberOfUserInputFiles);
        Job job3 = Job.getInstance(conf3, "TF-IDF of Words in Corpus");
        job3.setJarByClass(TFIDFDriver.class);
        job3.setMapperClass(WordsInAllDocsTFIDFMapper.class);
        job3.setReducerClass(WordsInAllDocsTFIDReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job3, new Path(OUTPUT_PATH_STAGE2));
        TextOutputFormat.setOutputPath(job3,finalOutput);

        return job3.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String... args) throws Exception {
        int runStatus = ToolRunner.run(new Configuration(), new TFIDFDriver(), args);
        System.exit(runStatus);
    }

    private void deletePaths(String path,FileSystem fs) throws IOException {
        final Path filepath = new Path(path);
        if(fs.exists(filepath)){
            System.out.println("Deleting a path: " + path);
            fs.delete(filepath,true);
        }
    }
}
