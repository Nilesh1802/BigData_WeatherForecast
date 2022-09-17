package in.neebal.avro.weather;

import in.neebal.avro.weatherdata.WeatherData;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempFinderMapper
		extends
		Mapper<AvroKey<Integer>, AvroValue<WeatherData>, IntWritable, IntWritable> {
	IntWritable key = new IntWritable();
	IntWritable value = new IntWritable();

	public void map(AvroKey<Integer> ikey, AvroValue<WeatherData> ivalue,
			Context context) throws IOException, InterruptedException {
		key.set(ikey.datum());
		value.set(ivalue.datum().getTemperature());
		context.write(key, value);
	}

}
