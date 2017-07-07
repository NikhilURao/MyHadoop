import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class MovieClass {
	public static class MapperMovie extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context con)
		{
		String[] s = value.toString().split(",");
		int movieyear=Integer.valueOf(s[2]);
		String moviename = s[1];
		try
		{
		con.write(new Text(moviename), new IntWritable(movieyear));
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		}
		
	}
	

public static class ReducerMovie extends Reducer<Text,IntWritable,Text,Text>
{
	public void reduce(Text key,Iterable<IntWritable>value,Context con) throws IOException, InterruptedException
	{
	int count=0;
		for (IntWritable it: value)
		{
			if(it.get()>1945 && it.get()<1959)
			{
			
	
			              count=count+1;
			               
				
			}
		
		}// end of for
		String output="the number of movies are"+ count;
		con.write("Text", new Text(output));
		
	}

}
	
	
	
	
public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf , "Movies number");
	job.setJarByClass(MovieClass.class);
	job.setMapperClass(MapperMovie.class);
	job.setReducerClass(ReducerMovie.class);
	job.setNumReduceTasks(1);

	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}