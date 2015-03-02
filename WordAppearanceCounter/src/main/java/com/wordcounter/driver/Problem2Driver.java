package com.wordcounter.driver;

import com.wordcounter.mapper.Stage1WordAppearanceMapper;
import com.wordcounter.mapper.WordAppearanceCounterMapper;
import com.wordcounter.reducer.Stage1WordAppearanceCounterReducer;
import com.wordcounter.reducer.WordAppearanceTotalReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Problem2Driver extends Configured implements Tool {
    public static void main(String... args) throws Exception {

        ToolRunner.run(new Configuration(),new Problem2Driver(),args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        if(args[0]==null||args[1]==null ){
           System.out.println("Please Specify following program arguments: \n <word1> <word2> <inputDirectoryInHDFS> <outputDirectoryInHDFS>");
            System.exit(0);
        }
        conf.set("word1",args[0]);
        conf.set("word2",args[1]);

        Job job = Job.getInstance(conf,"MapReduce 1");
        job.setJarByClass(Problem2Driver.class);
        job.setMapperClass(Stage1WordAppearanceMapper.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(Stage1WordAppearanceCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[2]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.waitForCompletion(true);
        return 0;
    }
}
