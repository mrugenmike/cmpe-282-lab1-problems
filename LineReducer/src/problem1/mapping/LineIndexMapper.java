package problem1.mapping;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LineIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		// Compile all the words using regex
		Pattern p = Pattern.compile("\\w+");
		Matcher m = p.matcher(value.toString().trim());
		System.out.println("Current Key:"+key+" value:"+value.toString());

		// Get the name of the file from the inputsplit in the context
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		// build the values and write <k,v> pairs through the context
		StringBuilder valueBuilder = new StringBuilder();
		while (m.find()) {
			String matchedKey = m.group().toLowerCase().trim();
			System.out.println("MatchedKey: "+matchedKey);
			// remove special characters and digits
			if (!Character.isLetter(matchedKey.charAt(0))
					|| Character.isDigit(matchedKey.charAt(0))) {
				continue;
			}
			valueBuilder.append(fileName);
			valueBuilder.append("@");
			valueBuilder.append(key.get());
			// emit the partial <k,v>
			
			context.write(new Text(matchedKey),new Text(valueBuilder.toString()));
			valueBuilder.setLength(0);
		}
	}

}
