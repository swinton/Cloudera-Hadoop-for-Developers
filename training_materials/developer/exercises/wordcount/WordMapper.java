import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordMapper extends MapReduceBase 
		implements Mapper<Object, Text, Text, IntWritable> {
	
	private Text word = new Text();
	private final static IntWritable ONE = new IntWritable(1);
	
	public void map(Object key, 
			Text value, 
			OutputCollector<Text, IntWritable> output, 
			Reporter reporter) throws IOException {
		
		// Break line into words for processing
		StringTokenizer wordList = new StringTokenizer(value.toString());
		
		while (wordList.hasMoreTokens()) {
			word.set(wordList.nextToken());
			output.collect(word, ONE);
		}
	}
}
