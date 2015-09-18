package com.som.hadoop.mapreduce2.partitioner;

import java.io.IOException;
import java.util.regex.Pattern;

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

//yarn jar /tmp/som/mapreduce2-0.0.1-SNAPSHOT.jar com.som.hadoop.mapreduce2.partitioner.TotalOrderParttitonerDemo /ngdb/tmp/input/ /ngdb/tmp/output/totalorder1 50 /ngdb/tmp/partition_table
public class TotalOrderParttitonerDemo extends Configured implements Tool {

	public static class TotalOrderMapper extends
			Mapper<Text, Text, Text, IntWritable> {

		private static final Logger LOGGER = Logger
				.getLogger(TotalOrderMapper.class);
		private Text keyOut = new Text();
		private IntWritable valueOut = new IntWritable(1);

		@Override
		protected void map(Text key, Text value,
				Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// Pattern pattern = Pattern.compile("\\s+");
			Pattern pattern = Pattern.compile(",");
			for (String word : pattern.split(key.toString())) {
				LOGGER.info("Key: "+key+"     Values: "+ value);
				keyOut.set(word);
				context.write(keyOut, valueOut);
			}

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
			// TODO Auto-generated method stub
			int sum = 0;
			 LOGGER.info("Key: "+key+"     Values: "+ values);
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}

	}

	public static void main(String[] args) {

		try {
			int exitSttus = ToolRunner.run(new TotalOrderParttitonerDemo(),
					args);
			System.exit(exitSttus);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 4) {
			System.err
					.println("Usage: com.som.hadoop.mapreduce2.partitioner.TotalOrderParttitonerDemo <input-path> <output-path> <numReduceTasks> <parttion_range>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path rangeTable = new Path(args[3]);
		int numReducetasks = Integer.parseInt(args[2]);
		
		Configuration conf=getConf();
//		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");//key value separator from '\t' to ','
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		
		Job job = Job.getInstance(conf, "Total Order Partitoner Demo");
		job.setJarByClass(getClass());

		job.setMapperClass(TotalOrderMapper.class);
		job.setReducerClass(TotalOrderReducer.class);
		// Set combiner to avoid network saturation
		job.setCombinerClass(TotalOrderReducer.class);

		job.setNumReduceTasks(numReducetasks);

		// set mapper/reducer class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);//default key value separator is '\t'
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// Partitioner
		job.setPartitionerClass(TotalOrderPartitioner.class);

		// TotalOrderPartitioner.setPartitionFile(conf, rangeTable); // don't
		// use this statement as it we have written
		// InputSampler.writePartitionFile(job, sampler);
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
				rangeTable);

		// We can use RandomSampler, IntervalSampler or SplitSampler
		// inputformat key type and sampler key type must be same(mapper output key,value type), 
		// here keyvaluetextinputformat has key type Text and so keeping Text as mapper output key class, or vice versa
		InputSampler.Sampler<Text, IntWritable> sampler = new InputSampler.RandomSampler<Text, IntWritable>(
				10.0, 1000000, 100);
		InputSampler.writePartitionFile(job, sampler);
		int exitStatus = job.waitForCompletion(true) ? 0 : 1;

		return exitStatus;
	}

}
