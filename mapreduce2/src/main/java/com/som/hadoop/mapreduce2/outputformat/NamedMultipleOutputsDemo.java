package com.som.hadoop.mapreduce2.outputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NamedMultipleOutputsDemo extends Configured implements Tool {

  public static class CtrPathFilter implements PathFilter {

    @Override
    public boolean accept(Path path) {
      // TODO Auto-generated method stub
      if (!path.toString().endsWith(".ctr")) {
        return true;
      }
      return false;
    }

  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    int exitCode = ToolRunner.run(new NamedMultipleOutputsDemo(), args);
    System.exit(exitCode);

  }

  @Override
  public int run(String[] args) throws Exception {
    // TODO Auto-generated method stub
    if (args.length != 2) {
      System.err.printf("Usage: %s <generic-args> <input> <output>", getClass()
          .getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
    }

    Job job = Job.getInstance(getConf(), "Named MultipleOutputs Demo");
    job.setJarByClass(NamedMultipleOutputsDemo.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.setInputPathFilter(job, CtrPathFilter.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(MultipleOutputsMapper.class);
    job.setReducerClass(MultipleOutputsReducer.class);
    
    job.setInputFormatClass(TextInputFormat.class);
    MultipleOutputs.addNamedOutput(job, "textfile", TextOutputFormat.class, NullWritable.class, Text.class);
//    MultipleOutputs.addNamedOutput(job, "lazyfile", LazyOutputFormat.class, NullWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "seqfile", SequenceFileOutputFormat.class, NullWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "mapfile", MapFileOutputFormat.class, NullWritable.class, Text.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;

  }

  public static class MultipleOutputsMapper extends
      Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value,
        Mapper<LongWritable, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
      context.write(new Text(value.toString().split(",")[2]), value);
    }
  }

  public static class MultipleOutputsReducer extends
      Reducer<Text, Text, NullWritable, Text> {

    private static MultipleOutputs<NullWritable, Text> mos;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        mos.write(
            NullWritable.get(),
            value,
            "simple/"+String.valueOf(key.toString().length() > 0 ? key.toString().charAt(
                0) : key));
        
        mos.write("textfile",NullWritable.get(),value,"text/part");
//        mos.write("lazyfile",NullWritable.get(),value,"lazy/part");
        mos.write("seqfile",NullWritable.get(),value,"seq/part");
        mos.write("mapfile",NullWritable.get(),value,"map/part");
      
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      mos.close();
    }
  }

}
