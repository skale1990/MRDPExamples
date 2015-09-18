package com.som.hadoop.mapreduce2.hdfs;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileReadFromHDFSDemo {
	private static Configuration configuration=null;
	
	static{
		configuration=new Configuration();
		configuration.set("fs.defaultFS", "hdfs://10.62.67.27:54310");
//		configuration.set("dfs.blocksize", "134217728");//128 MB
//		configuration.set("dfs.replication", "1");
	}
	public static void main(String[] args) throws Exception {
		
		// /ngdb/es/tmp/expected_US_SGSN_1.csv D:\\Sample_Java\\temp.csv 
		String hdfsFile= args[0];
		File localfile = new File(args[1]);
		if (localfile.isDirectory()) {
			throw new Exception("Destination is a directory, expecting file");
		}
		OutputStream out = new BufferedOutputStream(new FileOutputStream(localfile));
		
		FileSystem fs = FileSystem.get(URI.create(hdfsFile), configuration);
		FSDataInputStream in = fs.open(new Path(hdfsFile), 4096);
		
		IOUtils.copyBytes(in, out, configuration, true);
		
	}

}
