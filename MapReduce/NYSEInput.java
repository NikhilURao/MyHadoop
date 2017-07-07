//import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;// long=64bit number and int=32bit number
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
//work space should be under the hadoop user root directory

public class NYSEInput {
	
	//mapper
	public static class InputMapClass extends Mapper<LongWritable,Text,LongWritable,Text>
//LongWritable-inputkey(offset of the record) and outputkey.
	   {
	      public void map(LongWritable key, Text value, Context context)
	      //context is used to write something onto hadoop
	      {	    	  
	         try{
	            context.write(key,value);// writing the input data ip key and value
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	//driver   
	public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    Job job = Job.getInstance(conf, "Volume Count");//creating a job by the name Volume Count
		    job.setJarByClass(NYSEInput.class);// specifying the class which has the main class/driver class
		    job.setMapperClass(InputMapClass.class);
		    job.setNumReduceTasks(0);// no reducers
		    job.setOutputKeyClass(LongWritable.class);//output
		    job.setOutputValueClass(Text.class);//output
		    FileInputFormat.addInputPath(job, new Path(args[0]));//
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}