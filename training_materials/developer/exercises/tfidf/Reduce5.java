import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce5 extends MapReduceBase 
		implements Reducer<Text, IntWritable, Text, IntWritable> {
	
	/* Sum reducer
 	 * For input:
 	 * 	(key, (n1, n2, ..., nn))
 	 * Output:
 	 * 	(key, (SumOfN1ThroughN))
 	 */

	private IntWritable outputValue = new IntWritable(0);

	public void reduce(Text key, 
		Iterator<IntWritable> values, 
		OutputCollector<Text, IntWritable> output, 
		Reporter reporter)
	throws IOException {

		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}

		outputValue.set(sum);
		output.collect(key, outputValue);
	}
}
