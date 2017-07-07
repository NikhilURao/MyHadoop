import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {
	public static class MapperCount extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
			StringTokenizer it = new StringTokenizer(value.toString());
			while(it.hasMoreTokens())
			{
				String myword = it.nextToken().toLowerCase();
				con.write(new Text(myword), new IntWritable(1));
			}
		}
	}

	public static class ReducerCount extends Reducer <Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> value,Context con) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable i: value)
			{
				sum+= i.get();
			}
		con.write(key,new IntWritable (sum));
		}
	}
public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf = new Configuration();
	Job job = new Job(conf, "Word Count");
	job.setJarByClass(WordCount.class);
	job.setMapperClass(MapperCount.class);
	job.setReducerClass(ReducerCount.class);
	job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
}

}
	


