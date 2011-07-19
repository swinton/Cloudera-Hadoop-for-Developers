import java.io.IOException;
import java.util.Iterator;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InvertedIndexReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> {
	
	private Text locations = new Text();
	private final String SEP = ",";
	
	public void reduce(Text key, Iterator<Text> values, 
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		StringBuffer sb = new StringBuffer();
		while (values.hasNext()) {
			sb.append(values.next().toString());
			sb.append(SEP);
		}
		
		locations.set(sb.substring(0, sb.length() - 1));
		output.collect(key, locations);
	}
	
}
