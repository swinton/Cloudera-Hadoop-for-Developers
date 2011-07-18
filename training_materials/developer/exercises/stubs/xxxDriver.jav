import org.apache.hadoop.fs.Path;

// xxx Import types for intermediate key and value, output key and value.

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class xxxDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
	
		if (args.length != 2) {
			System.out.printf(
				"Usage: %s [generic options] <indir> <output dir>\n", 
				getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		JobConf conf = new JobConf(getConf(), xxxDriver.class);
		conf.setJobName("xxxDriver");

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(xxxMapper.class);
		conf.setReducerClass(xxxReducer.class);

		conf.setMapOutputKeyClass(xxxType.class);
		conf.setMapOutputValueClass(xxxType.class);

		conf.setOutputKeyClass(xxxType.class);
		conf.setOutputValueClass(xxxType.class);

		JobClient.runJob(conf);
		return 0;
	}
		
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new xxxDriver(), args);
		System.exit(exitCode);
	}
}
