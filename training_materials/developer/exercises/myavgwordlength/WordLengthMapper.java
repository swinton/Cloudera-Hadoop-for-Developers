import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class WordLengthMapper extends MapReduceBase 
	implements Mapper<Object, Text, Text, IntWritable> {
	
	private Text chr = new Text();
	
	public void map(Object key, 
			Text value, 
			OutputCollector<Text, IntWritable> output, 
			Reporter reporter) throws IOException {
		
		// Break line into words for processing
		StringTokenizer wordList = new StringTokenizer(value.toString());
		String word;
		while (wordList.hasMoreTokens()) {
			word = wordList.nextToken().toLowerCase();
			chr.set(word.substring(0, 1));
			// Emit the output
			output.collect(chr, new IntWritable(word.length()));
		}
	}
}
