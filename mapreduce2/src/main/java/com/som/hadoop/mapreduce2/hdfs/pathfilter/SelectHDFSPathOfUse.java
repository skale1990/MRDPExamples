package com.som.hadoop.mapreduce2.hdfs.pathfilter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SelectHDFSPathOfUse {

  private static Configuration configuration = null;

  static {
    configuration = new Configuration();

    configuration.set("fs.defaultFS", "hdfs://10.62.67.27:54310");

    // configuration.set("dfs.blocksize", "134217728");//128 MB
    // configuration.set("dfs.replication", "1");
  }

  public static void main(String[] args) {
    // /ngdb/*/*CEI*INDEX* 2G false
    String pathRegex = args[0];
    String pathRegexToBeExcluded = args[1];
    boolean include = Boolean.parseBoolean(args[2]);

    FileSystem fs = null;
    try {
      fs = FileSystem.get(configuration);
      // FileStatus[] status = fs.globStatus(new Path(pathRegex));
      FileStatus[] status =
          fs.globStatus(new Path(pathRegex), new RegexExcludeFilter(
              pathRegexToBeExcluded, include));

      for (FileStatus fileStatus : status) {
        System.out.println(fileStatus.getPath());
      }

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
