import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text word = new Text();
	private final static IntWritable ONE = new IntWritable(1);
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Break line into words for processing
		StringTokenizer wordList = new StringTokenizer(value.toString());
		
		while (wordList.hasMoreTokens()) {
			word.set(wordList.nextToken());
			context.write(word, ONE);
		}
	}
}
