package in.neebal.avro.weather;
import in.neebal.avro.weather.MaxTempFinderAvroMapper;
import in.neebal.avro.weather.MaxTempFinderAvroReducer;
import in.neebal.avro.weatherdata.MaxTempYear;
import in.neebal.avro.weatherdata.WeatherData;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MaxTempFinderAvroDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf);
		job.setJobName("MaxTempFinderAvroJob");
		job.setJarByClass(MaxTempFinderAvroDriver.class);
		AvroJob.setMapperClass(job, MaxTempFinderAvroMapper.class);
		AvroJob.setReducerClass(job, MaxTempFinderAvroReducer.class);
		AvroJob.setInputSchema(job, WeatherData.SCHEMA$);
		AvroJob.setMapOutputSchema(job, Pair.getPairSchema(
				Schema.create(Schema.Type.INT), WeatherData.SCHEMA$));

		AvroJob.setOutputSchema(job, MaxTempYear.SCHEMA$);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(
				"/root/Documents/vasu/hadoop/avrofiles"));
		FileOutputFormat
				.setOutputPath(
						job,
						new Path(
								"/root/Documents/vasu/hadoop/weather_sensor_output/avro_output"));
		JobClient.runJob(job);
	}

}
