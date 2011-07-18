import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

// xxx Import types for intermediate key and value, output key and value.

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class xxxDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
	
		if (args.length != 2) {
			System.out.printf(
				"Usage: %s [generic options] <indir> <outdir>\n", 
				getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		Job job = new Job(getConf(), "xxxDriver");
		job.setJarByClass(xxxDriver.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(xxxMapper.class);
		job.setReducerClass(xxxReducer.class);

		job.setMapOutputKeyClass(xxxType.class);
		job.setMapOutputValueClass(xxxType.class);

		job.setOutputKeyClass(xxxType.class);
		job.setOutputValueClass(xxxType.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
		
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new xxxDriver(), args);
		System.exit(exitCode);
	}
}
