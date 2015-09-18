package com.som.hadoop.mapreduce2.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgressDemo {

  public static void main(String[] args) throws Exception {
    // D:\\SVN\\CEM3.0_EP1.2\\packages\\ExpectedResults_SGSN\\ExpectedResults\\expected_US_SGSN_1.csv
    String localFile = args[0];
    // /ngdb/es/expected_US_SGSN_1.csv
    String hdfsPath = args[1];

    // create buffer for local file
    InputStream in = new BufferedInputStream(new FileInputStream(localFile));

    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "hdfs://10.62.67.27:54310");
    configuration.set("dfs.blocksize", "134217728");// 128 MB
    configuration.set("dfs.replication", "1");

    FileSystem fs = FileSystem.get(URI.create(hdfsPath), configuration);
    FSDataOutputStream out =
        fs.create(new Path(hdfsPath), false, 4096, new Progressable() {

          @Override
          public void progress() {
            // TODO Auto-generated method stub
            System.out.print("#");
          }
        });
    long start = System.currentTimeMillis();
    IOUtils.copyBytes(in, out, 4096, true);
    System.out.println((System.currentTimeMillis() - start) / 1000 * 60);
  }

}
