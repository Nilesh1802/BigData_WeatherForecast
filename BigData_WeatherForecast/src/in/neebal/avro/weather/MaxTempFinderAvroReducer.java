package in.neebal.avro.weather;

import java.io.IOException;
import java.util.Iterator;

import in.neebal.avro.weatherdata.MaxTempYear;
import in.neebal.avro.weatherdata.WeatherData;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.hadoop.mapred.Reporter;

public class MaxTempFinderAvroReducer extends AvroReducer<Integer, WeatherData, MaxTempYear> {
@Override
public void reduce(Integer yearKey, Iterable<WeatherData> value,
		AvroCollector<MaxTempYear> collector, Reporter arg3) throws IOException {
	Iterator<WeatherData> iterator=value.iterator();
	int maxTemp=0;
	int hour=0;
	if(iterator.hasNext())
	{
		maxTemp=iterator.next().getTemperature();
		hour=iterator.next().getHour();
	}
	while(iterator.hasNext())
	{
		WeatherData weatherData=iterator.next();
		int temp=weatherData.getTemperature();
		int tempHour=weatherData.getHour();
		if(temp>maxTemp)
		{
			maxTemp=temp;
			hour=tempHour;
		}
	}
	MaxTempYear maxTempYear=new MaxTempYear(yearKey,maxTemp,hour);
	collector.collect(maxTempYear);
}
}
