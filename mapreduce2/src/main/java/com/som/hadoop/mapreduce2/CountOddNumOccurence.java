package com.som.hadoop.mapreduce2;

import java.util.regex.Pattern;

import org.apache.hadoop.util.StringUtils;

public class CountOddNumOccurence {

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    int intArray[] = new int[] { 1, 1, 2, 3, 2, 3, 2, 3, 2, 3, 10 };
    int number = 0;
    for (int i : intArray) {
      number ^= i;
    }
    System.out.println(number + " occured odd number of times");

    System.out.println(Long.toBinaryString(-128));
    
  }

}
