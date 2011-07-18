import java.io.IOException;

// xxx Import types for input and output keys and values.

import org.apache.hadoop.mapreduce.Reducer;

public class xxxReducer extends Reducer<xxxType, xxxType, xxxType, xxxType> {
	
	public void reduce(xxxType key, 
		Iterable<xxxType> values, 
		Context context)
	throws IOException, InterruptedException {
		
		for (xxxType val : values) {
			// xxx Consume values in term val.
		}
		
		// xxx For output(s):
		// xxx   context.write(key, value);
	}
}
