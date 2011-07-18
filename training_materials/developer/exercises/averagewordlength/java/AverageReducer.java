import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AverageReducer extends MapReduceBase 
		implements Reducer<Text, IntWritable, Text, FloatWritable> {
	
	private FloatWritable avg = new FloatWritable();
	
	public void reduce(Text key, 
		Iterator<IntWritable> values, 
		OutputCollector<Text, FloatWritable> output, 
		Reporter reporter)
	throws IOException {
		
		int wordCount = 0;		
		int letterCount = 0;
		while (values.hasNext()) {
			wordCount++;
			letterCount += values.next().get();
		}
		
		avg.set((float) letterCount / wordCount);
		output.collect(key, avg);
	}
}
