import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map1 extends MapReduceBase 
		implements Mapper<Text, Text, Text, Text> {

	/* For input:
 	 *	(docName@offset, lineContent)
 	 * Output:
 	 * 	(docName, -)
 	 * , with "-" above being no value.
 	 */

	private static final Text noValue = new Text();
	private Text outputKey = new Text();
	
	public void map(Text key, 
			Text value, 
			OutputCollector<Text, Text> output, 
			Reporter reporter) 
	throws IOException {

		// Get key up to "@" character
		String[] keyParts = key.toString().split("@", 2);
		String docName = keyParts[0];
		
		outputKey.set(docName);
	
		output.collect(outputKey, noValue);

	}
}
