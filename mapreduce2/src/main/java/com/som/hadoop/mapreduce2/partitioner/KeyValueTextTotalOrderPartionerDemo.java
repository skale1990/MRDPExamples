package com.som.hadoop.mapreduce2.partitioner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

//yarn jar /tmp/som/mapreduce2-0.0.1-SNAPSHOT.jar com.som.hadoop.mapreduce2.partitioner.TotalOrderParttitonerDemo /ngdb/tmp/output/wordcount/ /ngdb/tmp/output/totalorder/ 15 /ngdb/tmp/output/partiton_file
public class KeyValueTextTotalOrderPartionerDemo extends Configured implements Tool {

	public static class TotalOrderMapper extends
			Mapper<Text, Text, Text, IntWritable> {

		private static final Logger LOGGER = Logger
				.getLogger(TotalOrderMapper.class);
		
		@Override
		protected void map(Text key, Text value,
				Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(key, new IntWritable(Integer.parseInt(value.toString())));
		}
	}

	public static class TotalOrderReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private static final Logger LOGGER = Logger
				.getLogger(TotalOrderReducer.class);

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
//			LOGGER.info("Word: " + values);
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}

	}

	public static void main(String[] args) {

		try {
			int exitSttus = ToolRunner.run(new KeyValueTextTotalOrderPartionerDemo(),
					args);
			System.exit(exitSttus);

		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 4) {
			System.err
					.println("Usage: com.som.hadoop.mapreduce2.partitioner.KeyValueTextTotalOrderPartionerDemo <input-path> <output-path> <numReduceTasks>");
			System.exit(-1);
		}
		Configuration conf = getConf();
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path rangeTable = new Path(args[3]);
		int numReducetasks = Integer.parseInt(args[2]);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath))
			fs.delete(outputPath, false);

		Job job = Job.getInstance(conf, "Total Order Partitoner Demo");
		job.setJarByClass(getClass());

		job.setMapperClass(TotalOrderMapper.class);
		job.setReducerClass(TotalOrderReducer.class);
		// Set combiner to avoid network saturation
		job.setCombinerClass(TotalOrderReducer.class);

		job.setNumReduceTasks(numReducetasks);

		// set mapper/reducer output class
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//Partitioner 
		job.setPartitionerClass(TotalOrderPartitioner.class);

//		TotalOrderPartitioner.setPartitionFile(conf, rangeTable);
//		conf.set(TotalOrderPartitioner.PARTITIONER_PATH, args[3]);
		
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), rangeTable);
		
		InputSampler.Sampler<Text, IntWritable> sampler = new InputSampler.RandomSampler<Text, IntWritable>(
				0.1, 1000, 10);
		InputSampler.writePartitionFile(job, sampler);
		int exitStatus = job.waitForCompletion(true) ? 0 : 1;

		return exitStatus;
	}

}

