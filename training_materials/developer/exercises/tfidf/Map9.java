import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map9 extends MapReduceBase 
		implements Mapper<Text, Text, Text, DoubleWritable> {

	/* Final Mapper to calculate tf-idf
 	 * Input is:
 	 * 	(word@docName, n@m@w)
 	 * , where:
 	 * 	n is the number of occurences of word in doc
 	 * 	m is the number of documents containing word
 	 * 	w is the number of words in doc
 	 *
 	 * Mapper also gets docN--the number of documents--from context
 	 *
 	 * Formulae: For any word@docName:
 	 * 	tf = n/w             "term frequency"
 	 * 	idf = log(docN/m)    "inverse document frequency"
 	 * 	tf-idf = tf * idf
 	 *
 	 * Output uses n, m, w, and docN to and emit:
 	 * 	(word@docName, tf-idf)
 	 */

	// Key is word@docName
	private int docN; // Num documents in corpus
	private int n;    // Num of occurances of word in this document
	private int m;    // Num of documents containing this word
	private int w;	  // Num of words in this document
	private DoubleWritable outputValue = new DoubleWritable(0.0f);

	public void configure(JobConf conf) {

		// Get docN from job config
		String strDocCount = conf.get("mapred.mapper.documentCount");
		docN = Integer.parseInt(strDocCount);
	}
	
	public void map(Text key, 
			Text value, 
			OutputCollector<Text, DoubleWritable> output, 
			Reporter reporter) 
	throws IOException {
		
		String valueStr = value.toString();
		String[] valueParts = valueStr.split("@", 3);
		n = Integer.parseInt(valueParts[0]);
		m = Integer.parseInt(valueParts[1]);
		w = Integer.parseInt(valueParts[2]);
		
		double tf = (double)n / w;
		double idf = Math.log10((double)docN / m);
		double tfidf = tf * idf;

		outputValue.set(tfidf);
		
		output.collect(key, outputValue);
	}
}
