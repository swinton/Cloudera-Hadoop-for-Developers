import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Job6 extends Configured implements Tool {

	public int run(String[] args) throws Exception {
	
		if (args.length != 3) {
			System.out.printf(
				"Usage: %s [generic options] <indir> <indir> <outdir>\n", 
				getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		JobConf conf = new JobConf(getConf(), Job6.class);
		conf.setJobName("Job6");

		conf.setInputFormat(KeyValueTextInputFormat.class);

		// Two input paths, to run a reduce-side join
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileInputFormat.addInputPath(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		conf.setMapperClass(Map6.class);
		conf.setReducerClass(Reduce6.class);

		conf.setPartitionerClass(FirstKeyPartPartitioner.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		JobClient.runJob(conf);
		return 0;
	}
		
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Job6(), args);
		System.exit(exitCode);
	}
}
