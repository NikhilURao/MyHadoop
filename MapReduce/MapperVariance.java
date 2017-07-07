import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperVariance extends Mapper<LongWritable,Text,Object,Object>
{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		
		String []lineParts=line.split(",");
		
		float hMax=Float.parseFloat(lineParts[4]);
		float lMax=Float.parseFloat(lineParts[5]);

		
		
		
		
	}
	
	

}
