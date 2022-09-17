import '/root/Documents/vasu/hadoop/PlayPig/max_in_group.macro';

weather_records = load '/root/Documents/vasu/hadoop_required_files_neebal/weather_data_input/json'
	using JsonLoader('stationNo:int,year:int,day:int, month:int, hours:int, minutes:int, temp:int');

filtered_records = filter weather_records by temp is not null;

projected_records = foreach filtered_records generate year,temp;

--Using macro
max_temp_year = max_in_group(projected_records, year, temp);

store max_temp_year into '/root/Documents/vasu/hadoop/pig/weather_data/output_with_coma' using PigStorage(',');