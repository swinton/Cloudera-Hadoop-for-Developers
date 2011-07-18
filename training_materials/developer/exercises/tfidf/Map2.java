import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map2 extends MapReduceBase 
		implements Mapper<Object, Text, IntWritable, IntWritable> {

	/* Key counter mapper
  	 * For input:
  	 * 	(key, val)
  	 * Output:
  	 * 	(1, 1)
  	 * 
  	 * (The reducer will count number of inputs that the mapper got.
  	 */
	
	private static final IntWritable one = new IntWritable(1);
	
	public void map(Object key, 
			Text value, 
			OutputCollector<IntWritable, IntWritable> output, 
			Reporter reporter) 
	throws IOException {

		output.collect(one, one);
		
	}
}
