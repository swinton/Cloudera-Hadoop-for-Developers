import java.io.IOException;

// xxx Import type for input and output keys and values.

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class xxxMapper extends MapReduceBase 
		implements Mapper<xxxType, xxxType, xxxType, xxxType> {
	
	public void map(xxxType key, 
			xxxType value, 
			OutputCollector<xxxType, xxxType> output, 
			Reporter reporter) 
	throws IOException {
		
		// xxx Consume input.  For output(s):
		// xxx    output.collect(xxxOutputKey, xxxOutputValue);

	}
}
