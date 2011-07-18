import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Job9 extends Configured implements Tool {

	JobConf conf;

	public int run(String[] args) throws Exception {
	
		if (args.length != 3) {
			System.out.printf(
				"Usage: %s [generic options] <docCountDir> <indir> <outdir>\n", 
				getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		conf = new JobConf(getConf(), Job9.class);
		conf.setJobName("Job9");

		conf.setInputFormat(KeyValueTextInputFormat.class);

		// Get document count from previous job
		String documentCount = getFirstWord(args[0]);

		// Put documentCount into config for mappers
		conf.set("mapred.mapper.documentCount", documentCount);
		
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		conf.setMapperClass(Map9.class);
		// Use default (identity) reducer

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DoubleWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		JobClient.runJob(conf);
		return 0;
	}
		
	public String getFirstWord(String inputDir) 
	throws IOException {

		// Return first word from the first file in the 
		// named input directory.

		// Navigate directory to find file
		Path directory = new Path(inputDir);
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] statList = fs.listStatus(directory);
		Path[] pathList = FileUtil.stat2Paths(statList);
		Path p = pathList[0];

		// Read integer from the file
		InputStream in = fs.open(p);
		BufferedReader br = new BufferedReader(
			new InputStreamReader(in));
		String textLine = br.readLine();
		StringTokenizer st = new StringTokenizer(textLine);

		return st.nextToken();
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Job9(), args);
		System.exit(exitCode);
	}
}
