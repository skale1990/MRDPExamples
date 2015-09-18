package com.som.hadoop.mapreduce2.hdfs.pathfilter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexExcludeFilter implements PathFilter{
	
	private final String regex;
	private final boolean include;
	
	public RegexExcludeFilter(String regex, boolean include) {
		this.regex = "^.*"+regex+".*$";
		this.include = include;
	}

	@Override
	public boolean accept(Path path) {
		// TODO Auto-generated method stub
		return path.toString().matches(regex)?include:!include;
	}

}
