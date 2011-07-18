import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, Text, Text, Text> {

	private static final String SEP = ",";
	
	public void reduce(Text key, 
		Iterable<Text> values, 
		Context context)
	throws IOException, InterruptedException {

		StringBuilder valueList = new StringBuilder();
		boolean firstValue = true;
		
		for (Text val : values) {

			if (!firstValue) {
				valueList.append(SEP);
			}

			valueList.append(val.toString());
			firstValue = false;
		}
		
		context.write(key, new Text(valueList.toString()));
	}
}
