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

	
	public class ReduceJoin {
	
		public static class MapperPurchase extends Mapper<LongWritable,Text,Text,Text>
		{
			public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
			{
				String[] line = value.toString().split(",");
				con.write(new Text (line[0]),new Text("pur\t"+line[1]));
			}
		}

		public static class MapperSales extends Mapper<LongWritable,Text,Text,Text>
		{
			public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
			{
				String[] line = value.toString().split(",");
				con.write(new Text (line[0]),new Text("sal\t"+line[1]));
			}
		}

		public static class ReducerAll extends Reducer <Text,Text,Text,Text>
		{
			public void reduce(Text key,Iterable<Text> value,Context con) throws IOException, InterruptedException
			{
				int totalpur= 0;
				int totalsal=0;
				for(Text it: value)
				{
					String [] line =it.toString().split("\t");
					if(line[0].equals("pur"))
					{
						totalpur += Long.parseLong(line[1]);
					}
					else if(line[0].equals("sal"))
					{
						totalsal += Long.parseLong(line[1]);
					}
				}
				String str = String.format("%d\t%d",totalpur,totalsal);
				con.write(key, new Text(str));
			}
		}
	public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(ReduceJoin.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJobName("Reduce side join");
		job.setReducerClass(ReducerAll.class);
		MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,MapperPurchase.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,MapperSales.class);
		
		Path outputpath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job,outputpath);
		System.exit(job.waitForCompletion(true) ? 0:1);
			
	}
	}
