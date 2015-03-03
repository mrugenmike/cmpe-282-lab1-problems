package problem1.driver;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import problem1.mapping.LineIndexMapper;
import problem1.reducer.LineIndexReducer;


public class Problem1Driver {
	
	public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		System.out.println("Started Job");
		Job job = Job.getInstance(conf, "wordcounter");
		job.setJarByClass(Problem1Driver.class);

		job.setMapperClass(LineIndexMapper.class);
		job.setReducerClass(LineIndexReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println("****Line Index Program Starts*****");
		Date startTime = new Date();
		System.out.println("Start Time: "+ startTime.toString());
		
		job.waitForCompletion(true);
		Date endTime = new Date();
		System.out.println("End Time: "+ endTime.toString());
		Long total = endTime.getTime()-startTime.getTime();
		
		System.out.println("Total Time Taken(Milliseconds): "+total);
		System.out.println("****End of Line Index Program*****");
		System.exit(0);
	}

}
