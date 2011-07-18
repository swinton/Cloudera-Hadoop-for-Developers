import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce4 extends MapReduceBase 
		implements Reducer<Text, Text, Text, IntWritable> {

	/* For input:
 	 * 	('key', ['val', 'val', 'val', ...])
 	 * Output:
 	 * 	('key', countOfValues)
 	 */

	private IntWritable outputValue = new IntWritable(0);
	
	public void reduce(Text key, 
		Iterator<Text> values, 
		OutputCollector<Text, IntWritable> output, 
		Reporter reporter)
	throws IOException {

		int valueCount = 0;
		
		while (values.hasNext()) {
			values.next();
			valueCount++;
		}
		
		outputValue.set(valueCount);
		output.collect(key, outputValue);
	}
}
