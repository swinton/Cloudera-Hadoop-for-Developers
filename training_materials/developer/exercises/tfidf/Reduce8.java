import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce8 extends MapReduceBase 
		implements Reducer<Text, Text, Text, Text> {

	/* Reducer for reduce-side 1-to-many join
 	 * Takes 2 kinds of keys:
 	 * 
 	 * For key type 1, remember value for join
 	 * 	For input like:
 	 * 		(docName@000, (w))
 	 * 	Produce *no* output, but retain w for type 2
 	 * 	
 	 * For key type 2, reinvert key and output join
 	 * 	For input like:
 	 * 		(docName@word, (n1@m1, n2@m2..., nn@mn))
 	 * 	Output:
 	 * 		(word@docName, n1@m1@w)
 	 * 		(word@docName, n2@m2@w)
 	 * 		...
 	 * 		(word@docName, nn@mn@w)
 	 *
 	 * Natural sort order ensures that a key of type 1
 	 * immediately precedes all keys of type 2 for the same
 	 * docName.  
 	 * A custom partitioner ensures that the two key types
 	 * arrive to the same reducer for the same docName.
 	 */

	private String wForJoin = new String();
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	public void reduce(Text key, 
		Iterator<Text> values, 
		OutputCollector<Text, Text> output, 
		Reporter reporter)
	throws IOException {

		String keyString = key.toString();

		if (keyString.indexOf("000") >= 0) {
			// key type 1: remember w
			wForJoin = values.next().toString();
		} else {
			// key type 2: invert key and emit 1-to-many join

			// first invert key
			String[] keyParts = keyString.split("@", 2);
			outputKey.set(keyParts[1] + "@" + keyParts[0]);

			// emit key with new joined value
			while (values.hasNext()) {
				outputValue.set(values.next() + "@" + wForJoin);
				output.collect(outputKey, outputValue);
			}
		}
	}
}
