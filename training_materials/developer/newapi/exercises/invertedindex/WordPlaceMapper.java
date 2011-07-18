import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class WordPlaceMapper extends Mapper<Object , Text, Text, Text> {

	private Text word = new Text();
	private Text outputValue = new Text();
	
	public void map(Object  key, 
		Text value, 
		Context context)
	throws IOException, InterruptedException {

		// The "new" mapreduce package does not have KeyValueTextInputFormat
		// in hadoop 0.20, so we're using TextInputFormat, and must grab
		// the desired "key" from the value line up to the tab character.
		
		String inputValue = value.toString();
		String[] valueParts = inputValue.split("\t" , 2);

		outputValue.set(valueParts[0]);
		StringTokenizer wordList = new StringTokenizer(valueParts[1]);

		while (wordList.hasMoreTokens()) {
			word.set(wordList.nextToken());
			context.write(word, outputValue);
		}
	}
}
