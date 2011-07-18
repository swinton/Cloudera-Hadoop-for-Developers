import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce3 extends MapReduceBase 
		implements Reducer<Text, Text, Text, Text> {

	/* For input:
 	 *	('keyPart1@keyPart2', '')
 	 * Output:
 	 * 	('keyPart1', 'keyPart2')
 	 */

	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	public void reduce(Text key, 
		Iterator<Text> values, 
		OutputCollector<Text, Text> output, 
		Reporter reporter)
	throws IOException {

		String[] keyParts = key.toString().split("@", 2);
		outputKey.set(keyParts[0]);
		outputValue.set(keyParts[1]);	
		output.collect(outputKey, outputValue);
	}
}
