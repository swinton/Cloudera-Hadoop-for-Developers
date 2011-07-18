import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map6 extends MapReduceBase 
		implements Mapper<Text, Text, Text, Text> {

	/* Mapper for reduce-side 1-to-many join.  Takes two kinds of keys.
 	 *
 	 * For key type 1, identity mapper
 	 * 	For input like:
 	 * 		(word@docName, n)
 	 * 	Output:
 	 * 		(word@docName, n)
 	 *
 	 * For key type 2, massage the key
 	 * 	For input like:
 	 * 		(word, m)
 	 * 	Output:
 	 * 		(word@000, m)
 	 *
 	 * A custom partitioner will partition these keys together.
 	 */

	private Text outputKey = new Text();
	
	public void map(Text key, 
			Text value, 
			OutputCollector<Text, Text> output, 
			Reporter reporter) 
	throws IOException {
		
		String keyString = key.toString();
		if (keyString.indexOf("@") >= 0) {
			// key type 1
			output.collect(key, value);
		} else {
			// key type 2
			outputKey.set(key + "@000");
			output.collect(outputKey, value);
		}
	}
}
