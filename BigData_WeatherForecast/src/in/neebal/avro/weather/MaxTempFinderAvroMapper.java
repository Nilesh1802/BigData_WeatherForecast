package in.neebal.avro.weather;

import java.io.IOException;

import in.neebal.avro.weatherdata.WeatherData;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;

public class MaxTempFinderAvroMapper extends AvroMapper<WeatherData, Pair<Integer,WeatherData>> {

	@Override
	public void map(WeatherData datum,
			AvroCollector<Pair<Integer, WeatherData>>collector,
			Reporter reporter) throws IOException{
			Pair<Integer, WeatherData> pair=new Pair<>(datum.getYear(),datum);
			collector.collect(pair);
	}
}




