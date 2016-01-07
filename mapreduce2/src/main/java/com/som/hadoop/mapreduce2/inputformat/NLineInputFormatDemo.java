package com.som.hadoop.mapreduce2.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.som.hadoop.mapreduce2.partitioner.TotalOrderParttitonerDemo.TotalOrderMapper;
import com.som.hadoop.mapreduce2.partitioner.TotalOrderParttitonerDemo.TotalOrderReducer;

public class NLineInputFormatDemo extends Configured implements Tool {

  public static void main(String[] args) {
    try {
      int exitStatus = ToolRunner.run(new NLineInputFormatDemo(), args);
      System.exit(exitStatus);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  @Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.err
					.println("Usage: com.som.hadoop.mapreduce2.inputformat.NLineInputFormatDemo <input-path> <output-path> <numReduceTasks>");
			System.exit(-1);
		}
		Configuration conf = getConf();
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		int numReducetasks = Integer.parseInt(args[2]);

		FileSystem fs = FileSystem.get(conf);
		
		if (fs.exists(outputPath))
			fs.delete(outputPath, false);
		
		Job job = Job.getInstance(conf, "NLineInputFormatDemo");
		job.setJarByClass(getClass());

//		job.setMapperClass(.class);
//		job.setReducerClass(.class);
	

		job.setNumReduceTasks(numReducetasks);

		// set mapper/reducer output class
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// set lines to be processed within a map
//		NLineInputFormat.
		int exitStatus = job.waitForCompletion(true) ? 0 : 1;

		return exitStatus;
	}
}
