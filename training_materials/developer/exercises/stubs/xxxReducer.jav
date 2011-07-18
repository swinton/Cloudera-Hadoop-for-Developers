import java.io.IOException;
import java.util.Iterator;

// xxx Import types for input and output keys and values.

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class xxxReducer extends MapReduceBase 
		implements Reducer<xxxType, xxxType, xxxType, xxxType> {
	
	public void reduce(xxxType key, 
		Iterator<xxxType> values, 
		OutputCollector<xxxType, xxxType> output, 
		Reporter reporter)
	throws IOException {
		
		while (values.hasNext()) {

			// xxx Consume values in term values.next()

		}
		
		// xxx For output(s):
		// xxx   output.collect(xxxOutputKey, xxxOutputValue);
	}
}
