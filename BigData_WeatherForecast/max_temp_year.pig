weather_records = load '$input_path'
	using PigStorage('|')
	as(station,year:int,daymonth, hourminute, temperature:int);
filtered_records = filter weather_records by temperature is not null;
projected_records = foreach filtered_records generate year,temperature;
grouped_records = group projected_records by year;
max_temp_year = foreach grouped_records generate group as year,MAX(projected_records.temperature);
store max_temp_year into '$output_path' using PigStorage(',');
--	command to run on terminal with parameters
--	pig -f '/root/Documents/vasu/hadoop/PlayPig/max_temp_year.pig' -param input_path=/user/vasu/weather_data_input/pipe_delimited_files -param output_path=/user/vasu/weather_data_output/pipe_delimited_files_ouput

--	command to run on terminal with parameters from a file
--  pig -f '/root/Documents/vasu/hadoop/PlayPig/max_temp_year.pig' -param_file '/root/Documents/vasu/hadoop/PlayPig/max_temp_year_parameters'
