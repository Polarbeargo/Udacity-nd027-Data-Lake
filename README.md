# Udacity-nd027-Data-Lake  

## Project Datasets
* Song data: 's3://udacity-dend/song_data'  
* Log data: 's3://udacity-dend/log_data'  
### Song Dataset  
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
### Log Dataset  
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.
The log files in the dataset will be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
## Fact Table  
1. songplays - records in log data associated with song plays i.e. records with page NextSong
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent  
## Dimension Tables  

2. users - users in the app  
   * user_id, first_name, last_name, gender, level  
3. songs - songs in music database    
   * song_id, title, artist_id, year, duration  
4. artists - artists in music database   
   * artist_id, name, location, latitude, longitude  
5. time - timestamps of records in songplays broken down into specific units  
   * start_time, hour, day, week, month, year, weekday  
  
## Project Template
   * `etl.py` reads data from S3, processes that data using Spark, and writes them back to S3.  
   * `dl.cfg` contains your AWS credentials.   
   * `README.md` provides discussion on your process and decisions.  

## Project Steps  
   * Building an ETL pipeline that extracts their data from S3.  
   * Processes them using Spark, and loads the data back into S3 as a set of dimensional tables.  
   * Load tables into s3 in parquet format.  

## How to run scripts

   * Set environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `dl.cfg`.  
   * Connect to S3 bucket and replace the output_data variable in the main() function with the S3 bucket URL.    
   * Run ETL pipeline.  

```bash
$ python etl.py
```  
