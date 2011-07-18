import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map5 extends MapReduceBase 
		implements Mapper<Text, Text, Text, IntWritable> {

	/* For input:
 	 *	(docName@offset, lineContent)
 	 * , break lineContent into words and output:
 	 * 	(word@docName, 1)
 	 * , where words pass WordFilter
 	 */

	private static final IntWritable one = new IntWritable(1);
	private Text outputKey = new Text();
	
	public void map(Text key, 
			Text value, 
			OutputCollector<Text, IntWritable> output, 
			Reporter reporter) 
	throws IOException {

		// Get key up to "@" character
		String[] keyParts = key.toString().split("@", 2);
		String docName = keyParts[0];
		
		// Break value into words
		StringTokenizer wordList = new StringTokenizer(value.toString());

		while (wordList.hasMoreTokens()) {
			String word = wordList.nextToken();
			if (WordFilter.isGood(word)) {
				word = WordFilter.word2GoodWord(word);
				outputKey.set(word + "@" + docName);
				output.collect(outputKey, one);
			}
		}
	}
}
