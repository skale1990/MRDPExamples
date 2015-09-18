package com.som.hadoop.mapreduce2;

import java.util.regex.Pattern;

public class CountOddNumOccurence {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int intArray[] = new int[]{1,1,2,3,2,3,2,3,2,3,10};
		int number=0;
		for (int i : intArray) {
			number^=i;
		}
		System.out.println(number+" occured odd number of times");
		
		Pattern pattern = Pattern.compile("^-?[0-9]{1,5}+(\\.[0-9]{1,0})?$");
		
	
	}

}
