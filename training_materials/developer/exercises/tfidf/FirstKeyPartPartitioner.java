import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Partitioner;

import org.apache.hadoop.mapred.lib.HashPartitioner;

public class FirstKeyPartPartitioner
		extends HashPartitioner<Text, Text>
		implements Partitioner<Text, Text> {

	/* Given keys of form: 
 	 * 	(word@part2)
 	 * 
 	 * , use the Partitioner being extended to
 	 * partition on
 	 *
 	 * 	(word)
 	 */

	public Text keyToUse = new Text();

	public int getPartition(Text key,
				Text value,
				int numPartitions) {

		String[] keyParts = key.toString().split("@", 2);
		String wordForPartitioning = keyParts[0];
		keyToUse.set(wordForPartitioning);

		return super.getPartition(keyToUse,
				value,
				numPartitions);
	}
}
