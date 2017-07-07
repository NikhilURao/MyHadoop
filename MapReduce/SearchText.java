import java.io.IOException;

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


public class SearchText {
	public static class MapperSearch extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text value,Context con) throws IOException, InterruptedException
		{
			String searchtext = con.getConfiguration().get("Search Text");
			String line = value.toString();
			String Line = line.toLowerCase();
			String Searchtext = searchtext.toLowerCase();
			
			if(searchtext != null)
			{
				if(Line.contains(Searchtext))
				{
					con.write(new Text(Line), new IntWritable(1));
				}
			}
		}
	}
	public static class ReducerSearch extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> value, Context con) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable i:value)
			{
				sum+= i.get();
				
			}
		con.write(key, new IntWritable(sum));
		}

	}
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf = new Configuration();
	
	if (args.length > 2)
	{
		conf.set("Search Text", args[2]);
	}
	else
	{
		System.out.println("Need more than 2 arguements");
	}
Job job = new Job(conf,"Search word");
job.setJarByClass(SearchText.class);
job.setMapperClass(MapperSearch.class);
job.setReducerClass(ReducerSearch.class);
job.setNumReduceTasks(1);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}