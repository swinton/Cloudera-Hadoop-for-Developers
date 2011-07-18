import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map3 extends MapReduceBase 
		implements Mapper<Text, Text, Text, Text> {
	
	/* For input record of form:
         *	('docname@offset', 'word1 word2 ... wordn')
         *
         * Output records:
         *	('docname@word1', '')
         *	('docname@word2', '')
         *	...
         *	('docname@word3', '')
         * , for words that pass the WordFilter.
         */

	private Text outputKey = new Text();
	private static final Text emptyValue = new Text("");
	
	public void map(Text key, 
			Text value, 
			OutputCollector<Text, Text> output, 
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
				outputKey.set(docName + "@" + word);
				output.collect(outputKey, emptyValue);
			}
		}
	}
}
