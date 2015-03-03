package problem1.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LineIndexReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text word, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder();
		
		System.out.println("Current Reducer Key:"+word+" values:"+values);
		
		for (Text value : values) {
			builder.append(value.toString());
			System.out.println("Curr Value: "+value);
			builder.append(",");	
		}
		context.write(word, new Text(builder.substring(0, builder.length() - 1)));
		builder.setLength(0);
	}
}
