import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text letter = new Text();
	private IntWritable wordLength = new IntWritable(1);
	
	public void map(Object key, 
		Text value, 
		Context context)
	throws IOException, InterruptedException {
		
		// Break line into words for processing
		StringTokenizer wordList = new StringTokenizer(value.toString());
		
		while (wordList.hasMoreTokens()) {
			String word = wordList.nextToken();
			letter.set(word.substring(0, 1));
			wordLength.set(word.length());
			context.write(letter, wordLength);
		}
	}
}
