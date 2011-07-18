import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AvgReducer extends MapReduceBase 
	implements Reducer<Text, IntWritable, Text, FloatWritable> {
	
	private FloatWritable totalWordAvg = new FloatWritable();
	
	public void reduce(Text key, Iterator<IntWritable> values, 
			OutputCollector<Text, FloatWritable> output, Reporter reporter)
			throws IOException {
		
		int occurrences = 0;
		int total = 0;
		float avg;
		while (values.hasNext()) {
			occurrences++;
			total += values.next().get();
		}
		
		avg = (float)total/(float)occurrences;
		
		totalWordAvg.set(avg);
		output.collect(key, totalWordAvg);
	}
}
