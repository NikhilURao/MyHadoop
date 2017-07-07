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

public class Speed {
public static  class MapperSpeed extends Mapper<LongWritable,Text,Text,IntWritable>
{
public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
{
String[] line = value.toString().split(",");	
int speed = Integer.valueOf(line[1]);
con.write(new Text(line[0]),new IntWritable(speed));
}
}


public static class ReducerSpeed extends Reducer<Text,IntWritable,Text,IntWritable>
{
public void reduce(Text key,Iterable<IntWritable> value, Context con) throws IOException, InterruptedException
{
	int vehicleEntry=0;
	int offence=0;
	for (IntWritable speed :value)
	{
		vehicleEntry++;
		if (speed.get() > 65)
		{
		offence++;	
		}
	}
int offencePercentage = (offence*100)/vehicleEntry;
con.write(key,new IntWritable(offencePercentage));
}
}


public static void main(String[] arg) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf = new Configuration();
    Job job = new Job (conf, "Vehicle speeds");
    job.setJarByClass(Speed.class);
    job.setMapperClass(MapperSpeed.class);
    job.setReducerClass(ReducerSpeed.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(arg[0]));
    FileOutputFormat.setOutputPath(job, new Path(arg[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}