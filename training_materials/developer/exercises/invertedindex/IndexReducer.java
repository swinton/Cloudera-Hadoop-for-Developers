import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class IndexReducer extends MapReduceBase 
		implements Reducer<Text, Text, Text, Text> {

	private static final String SEP = ",";
	
	public void reduce(Text key, 
		Iterator<Text> values, 
		OutputCollector<Text, Text> output, 
		Reporter reporter)
	throws IOException {

		StringBuilder valueList = new StringBuilder();
		boolean firstValue = true;
		
		while (values.hasNext()) {

			if (!firstValue) {
				valueList.append(SEP);
			}

			valueList.append(values.next().toString());
			firstValue = false;
		}
		
		output.collect(key, new Text(valueList.toString()));
	}
}
