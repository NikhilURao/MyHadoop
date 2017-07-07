import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceSideJoin {

	public static class MapperCust extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
			String[] line = value.toString().split(",");
			con.write(new Text (line[0]),new Text("cust\t"+line[1]));
		}
	}

	public static class MapperTrans extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
			String[] line = value.toString().split(",");
			con.write(new Text (line[2]),new Text("trans\t"+line[3]));
		}
	}

	public static class ReducerAll extends Reducer <Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> value,Context con) throws IOException, InterruptedException
		{
			double totaltrans= 0.0;
			int count =0;
			String name ="";
			for(Text it: value)
			{
				String [] line =it.toString().split("\t");
				if(line[0].equals("trans"))
				{
					count++;
					totaltrans += Float.parseFloat(line[1]);
				}
				else 
				{
					name = line[1];
				}
			}
			String str = String.format("%d\t%f",count,totaltrans);
			con.write(new Text(name), new Text(str));
		}
	}
public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf);
	job.setJarByClass(ReduceSideJoin.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setJobName("Reduce side join");
	job.setReducerClass(ReducerAll.class);
	MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,MapperCust.class);
	MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,MapperTrans.class);
	
	Path outputpath = new Path(args[2]);
	FileOutputFormat.setOutputPath(job,outputpath);
	System.exit(job.waitForCompletion(true) ? 0:1);
	
	
	
	
	
	
	
}
}
