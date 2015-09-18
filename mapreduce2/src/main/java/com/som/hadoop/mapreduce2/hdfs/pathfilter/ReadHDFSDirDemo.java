package com.som.hadoop.mapreduce2.hdfs.pathfilter;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ReadHDFSDirDemo {
	private static Configuration configuration = null;

	static {
		configuration = new Configuration();

		configuration.set("fs.defaultFS", "hdfs://10.62.67.27:54310");

		// configuration.set("dfs.blocksize", "134217728");//128 MB
		// configuration.set("dfs.replication", "1");
	}

	public static void main(String[] args) {
		String hdfsPath=null;
		String pathRegexToBeExcluded = null;
		boolean include=false;
		switch (args.length) {
		case 1:
			hdfsPath = args[0];
			break;
			
		case 3:
			hdfsPath = args[0];
			pathRegexToBeExcluded = args[1];
			include= Boolean.parseBoolean(args[2]);
			break;
		default:
			System.err.println("Usage: com.som.hadoop.mapreduce2.hdfs.pathfilter.ReadHDFSDirDemo <Comma seperated HDFS directories to read> [<PathFilterRegex> <include>]");
			System.err.println("Usage: com.som.hadoop.mapreduce2.hdfs.pathfilter.ReadHDFSDirDemo /ngdb/ps,/ngdb/es");
			System.err.println("Usage: com.som.hadoop.mapreduce2.hdfs.pathfilter.ReadHDFSDirDemo /ngdb/ps *CEI*INDEX* true");
			System.exit(-1);
			break;
		}
		
		
		String[] hdfsPathArray = hdfsPath.trim().split(",");
		Path [] paths = new Path[hdfsPathArray.length];
//		System.out.println(hdfsPathArray[0]);
		for (int j = 0; j < hdfsPathArray.length; j++) {
			paths[j] = new Path(hdfsPathArray[j]);
		}
	
		FileSystem fs = null;
		FileStatus[] status = null;
		try {
			fs=FileSystem.get(URI.create(hdfsPath),configuration);
			if (args.length == 1) {
				status = fs.listStatus(paths);
			}else{
				status = fs.listStatus(paths,new RegexExcludeFilter(pathRegexToBeExcluded, include));
			}
			paths = FileUtil.stat2Paths(status);
			
			for (Path path : paths) {
				System.out.println(path);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
