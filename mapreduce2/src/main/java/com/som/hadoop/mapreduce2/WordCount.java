package com.som.hadoop.mapreduce2;

import java.io.IOException;
import java.util.StringTokenizer;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class WordCount extends Configured implements Tool {

  public static class WordCountMapper extends
      Mapper<Object, Text, Text, IntWritable> {

    private static final Logger LOGGER = Logger
        .getLogger(WordCountMapper.class);
    private Text keyOut = new Text();
    private IntWritable valueOut = new IntWritable(1);

    @Override
    protected void map(Object key, Text value,
        Mapper<Object, Text, Text, IntWritable>.Context context)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      Pattern pattern = Pattern.compile("\\s+");
      for (String word : pattern.split(value.toString())) {
        LOGGER.info("Word: " + word);
        keyOut.set(word);
        context.write(keyOut, valueOut);
      }

      /*
       * StringTokenizer str =new StringTokenizer(value.toString(), "\\s+");
       * while (str.hasMoreTokens()) { keyOut.set(str.nextToken());
       * context.write(keyOut, valueOut); }
       */

    }
  }

  public static class WordCountReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {

    private static final Logger LOGGER = Logger
        .getLogger(WordCountReducer.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Reducer<Text, IntWritable, Text, IntWritable>.Context context)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      int sum = 0;
      LOGGER.info("Word: " + values);
      for (IntWritable value : values) {
        sum += value.get();
      }

      context.write(key, new IntWritable(sum));
    }

  }

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    try {
      int exitSttus = ToolRunner.run(conf, new WordCount(), args);
      System.exit(exitSttus);

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    // TODO Auto-generated method stub
    if (args.length != 2) {
      System.err
          .println("Usage: com.som.hadoop.mapreduce2.WordCount <input-path> <output-path>");
      System.exit(-1);
    }
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(outputPath))
      fs.delete(outputPath, true);

    Job job = Job.getInstance(getConf(), "Word Count Example");
    job.setJarByClass(getClass());

    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    // set mapper/reducer class
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    int exitStatus = job.waitForCompletion(true) ? 0 : 1;

    return exitStatus;
  }

}
