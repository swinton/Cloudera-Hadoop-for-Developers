import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce6 extends MapReduceBase 
		implements Reducer<Text, Text, Text, Text> {

	/* Reducer for reduce-side 1-to-many join
 	 * Takes 2 kinds of keys:
 	 * 
 	 * For key type 1, remember value for join
 	 * 	For input like:
 	 * 		(word@000, (m))
 	 * 	Produce *no* output, but retain m for type 2
 	 * 	
 	 * For key type 2, output join
 	 * 	For input like:
 	 * 		(word@docName, (n1, n2, ..., nn))
 	 * 	Output:
 	 * 		(word@docName, n1@m)
 	 * 		(word@docName, n2@m)
 	 * 		...
 	 * 		(word@docName, nm@m)
 	 *
 	 * Natural sort order ensures that a key of type 1
 	 * immediately precedes a keys of type 2 for the same word.
 	 * A custom partitioner ensures the two key types arrive at
 	 * the same reducer for the same word.
 	 */

	private String mForJoin = new String();
	private Text outputValue = new Text();
	
	public void reduce(Text key, 
		Iterator<Text> values, 
		OutputCollector<Text, Text> output, 
		Reporter reporter)
	throws IOException {

		String keyString = key.toString();

		if (keyString.indexOf("000") >= 0) {
			// key type 1: remember m
			mForJoin = values.next().toString();
		} else {
			// key type 2: emit 1-to-many join
			while (values.hasNext()) {
				outputValue.set(values.next() + 
					"@" + mForJoin);
				output.collect(key, outputValue);
			}
		}
	}
}
