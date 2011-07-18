import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce1 extends MapReduceBase 
		implements Reducer<Text, Text, Text, Text> {

	/* Distinct key reducer
         * For input:
         * 	(key, [val, val, ..., val])
         * Output:
         * 	(key, noValue)
         */

	private static final Text noValue = new Text();
	
	public void reduce(Text key, 
		Iterator<Text> values, 
		OutputCollector<Text, Text> output, 
		Reporter reporter)
	throws IOException {

		output.collect(key, noValue);
	}
}
