import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
	
	private FloatWritable avg = new FloatWritable();
	
	public void reduce(Text key, 
		Iterable<IntWritable> values, 
		Context context)
	throws IOException, InterruptedException {
		
		int wordCount = 0;		
		int letterCount = 0;
		for (IntWritable val : values) {
			wordCount++;
			letterCount += val.get();
		}
		
		avg.set((float) letterCount / wordCount);
		context.write(key, avg);
	}
}
