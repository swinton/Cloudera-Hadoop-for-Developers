import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce2 extends MapReduceBase 
		implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	/* Sum reducer, takes a single group for input, 
 	 * counts the number of values.
 	 * For input:
 	 * 	(key, (val, val, ..., val))
 	 * Output:
 	 *	(countOfValues, countOfValues)
 	 *
 	 * (Single count output in both parts for convenience.)
 	 */

	private IntWritable outputSum = new IntWritable(0);

	public void reduce(IntWritable key, 
		Iterator<IntWritable> values, 
		OutputCollector<IntWritable, IntWritable> output, 
		Reporter reporter)
	throws IOException {

		int sumValues = 0;
		while (values.hasNext()) {
			sumValues += values.next().get();
		}

		outputSum.set(sumValues);

		output.collect(outputSum, outputSum);
	}
}
