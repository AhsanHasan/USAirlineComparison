package org.usairlinecomparision;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class USAirlineComparision {

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if (args.length != 3)
		{
			System.err.println("Usage: USAirlineComparision <input path> <output path>");
			System.exit(-1);
		}
		Job job;
		job=Job.getInstance(conf, "USA");
		job.setJarByClass(USAirlineComparision.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(USAirlineComparisionMapper.class);
		job.setReducerClass(USAirlineComparisionReducer.class);
		job.setCombinerClass(USAirlineComparisionCombiner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
