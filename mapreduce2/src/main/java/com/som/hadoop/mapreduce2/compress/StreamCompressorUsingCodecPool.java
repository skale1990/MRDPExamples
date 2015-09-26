package com.som.hadoop.mapreduce2.compress;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;



public class StreamCompressorUsingCodecPool  {
  
  
  public static void main(String[] args) {
    if (args.length == 0) {
      StreamProcessorOptionsParser.printGenericCommandUsage(System.out);
      System.exit(0);
    }
    try {
     Configuration configuration = new Configuration();
      configuration.set("fs.defaultFS", "hdfs://10.62.67.27:54310");
      new StreamProcessorOptionsParser(configuration).parseAndProcessOptions(args);
        
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
