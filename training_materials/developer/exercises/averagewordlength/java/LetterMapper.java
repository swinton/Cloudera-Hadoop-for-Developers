import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class LetterMapper extends MapReduceBase 
		implements Mapper<Object, Text, Text, IntWritable> {
	
	private Text letter = new Text();
	private IntWritable wordLength = new IntWritable(1);
	
	public void map(Object key, 
		Text value, 
		OutputCollector<Text, IntWritable> output, 
		Reporter reporter) 
	throws IOException {
		
		// Break line into words for processing
		StringTokenizer wordList = 
			new StringTokenizer(value.toString());
		
		while (wordList.hasMoreTokens()) {
			String word = wordList.nextToken();
			letter.set(word.substring(0, 1));
			wordLength.set(word.length());
			output.collect(letter, wordLength);
		}
	}
}
