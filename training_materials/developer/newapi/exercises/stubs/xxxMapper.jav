import java.io.IOException;

// xxx Import types for input and output keys and values.

import org.apache.hadoop.mapreduce.Mapper;

public class xxxMapper extends Mapper<xxxType, xxxType, xxxType, xxxType> {
	
	public void map(xxxType key, 
		xxxType value, 
		Context context)
	throws IOException, InterruptedException {

		// xxx Consume input.  For output(s):
		// xxx    context.write(xxxKey, xxxValue);
		
	}
}
