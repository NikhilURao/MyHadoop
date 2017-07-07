package com.niit;
import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper <LongWritable,Text,Object,Object> 
{
@Override // already existing method of the Mapper class map is called 
protected void map(LongWritable key,Text value,Context apr1) throws IOException,InterruptedException
{
	String line = value.toString();// converting the records to String type
	String [] lineparts = line.split("\t");// specifying that each word in the record is separated by a tab or \t
	
	for(String it :lineparts)
	{
		if(it.equals("A"))
		{
			Text outKey= new Text(it);
			IntWritable outValue = new IntWritable(1);
			apr1.write(outKey, outValue);
		}
	}
	
}
}