import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordPlaceMapper extends MapReduceBase 
		implements Mapper<Text, Text, Text, Text> {

	private Text word = new Text();
	
	public void map(Text key, 
			Text value, 
			OutputCollector<Text, Text> output, 
			Reporter reporter) 
	throws IOException {

		StringTokenizer wordList = new StringTokenizer(value.toString());
		
		while (wordList.hasMoreTokens()) {
			word.set(wordList.nextToken());
			output.collect(word, key);
		}

	}
}
