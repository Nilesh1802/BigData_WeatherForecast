package in.neebal.avro.weather;

import in.neebal.avro.weatherdata.WeatherData;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTempFinderDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MaxTempFinderAvroKeyValue");
		job.setJarByClass(in.neebal.avro.weather.MaxTempFinderDriver.class);

		job.setMapperClass(MaxTempFinderMapper.class);
		job.setReducerClass(MaxTempFinderReducer.class);
		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		AvroJob.setInputKeySchema(job,Schema.create(Schema.Type.INT));
		AvroJob.setInputValueSchema(job, WeatherData.SCHEMA$);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:8020/user/vasu/weather_data_input/avro_keyvalue_files"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:8020/user/vasu/weather_data_output/avro_keyvalue_output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
