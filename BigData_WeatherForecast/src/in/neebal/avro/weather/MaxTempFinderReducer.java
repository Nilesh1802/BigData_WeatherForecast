package in.neebal.avro.weather;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempFinderReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	public void reduce(IntWritable year, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		Iterator<IntWritable> iterator=values.iterator(); 
		int maxTemp = 0;
		
		if(iterator.hasNext()){
			maxTemp = iterator.next().get();			
		}
		
		while(iterator.hasNext()){
			int tempTemp = iterator.next().get();
			if(tempTemp > maxTemp){
				maxTemp = tempTemp;
			}
		}
		context.write(year, new IntWritable(maxTemp));
	}

}
