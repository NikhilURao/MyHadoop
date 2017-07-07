import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class RetailStore1 {
	public static class RetailMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
			String [] line = value.toString().split(";");
			String cusid = line[1];
			double cost = Integer.valueOf(line[7]);
			con.write(new Text(cusid), new DoubleWritable(cost));
		}
	}
public static class RetailReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
{ 
	public void reduce(Text key,Iterable<DoubleWritable> value,Context con) throws IOException, InterruptedException
	{
	double max = Integer.MAX_VALUE;
	Iterator<DoubleWritable> it = value.iterator();
	while(it.hasNext())
	{
		double values = it.next().get();
		if(values>=max)
		{
			max = values;
		}
	}
	con.write(new Text(key),new DoubleWritable(max));
	}
}
	
	
	public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "top customer");
		job.setJarByClass(RetailStore1.class);
		job.setMapperClass(RetailMapper.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(RetailReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}