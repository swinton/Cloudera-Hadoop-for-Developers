import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map7 extends MapReduceBase 
		implements Mapper<Text, Text, Text, IntWritable> {

	/* For input:
 	 *	(docName@offset, lineContent)
 	 * Output:
 	 * 	(docName, wordcount)
 	 * , with "wordcount" above being the count of
 	 * good words in the line.
 	 */

	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable(0);
	
	public void map(Text key, 
			Text value, 
			OutputCollector<Text, IntWritable> output, 
			Reporter reporter) 
	throws IOException {

		// Get key up to "@" character
		String[] keyParts = key.toString().split("@", 2);
		String docName = keyParts[0];
		
		outputKey.set(docName);

		// Count the "good" words in the line
		int wordCount = 0;
		StringTokenizer wordList = new StringTokenizer(value.toString());

		while(wordList.hasMoreTokens()) {
			String word = wordList.nextToken();
			if (WordFilter.isGood(word)) {
				wordCount++;
			}
		}

		outputValue.set(wordCount);
	
		output.collect(outputKey, outputValue);

	}
}
