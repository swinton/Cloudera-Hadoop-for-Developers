import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map4 extends MapReduceBase 
		implements Mapper<Text, Text, Text, Text> {

	/* Inverse Mapper.  For input:
 	 *	(k, v)
 	 * Output:
 	 * 	(v, k)
 	 */
	
	public void map(Text key, 
			Text value, 
			OutputCollector<Text, Text> output, 
			Reporter reporter) 
	throws IOException {
		
		output.collect(value, key);

	}
}
