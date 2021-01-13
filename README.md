# exact-data-engineer-assignment
  This is my implementation for the assignment
  
  
  
# For Understanding the Project flow
   I have added an Assignment Documentation_main.xlsx and Assignment flow.xlsx which define the architecture,
     Project Flow , Data Dictionary in each of the files
      and Technologies used.



# To Start please run the following in order

1- In the "download_ny.py" file add URL's of all the files that you need to download in the URL's array and the months array as the default I presented.

2- Run the "downloader.py" file.

3- Wait for the files to download.

4- To create the Parquet file that merges all the data togather- Run the "exact_main.py" file.
     This will create a merge of the data in a Parquet File which will be our base file
     
5- Run the "ny_yc_cleaning_and_sql.py" file which will create a new Clean Parquet file after the cleaning process using pyspark

6- Run the "parquet_schema_and_output.py" file which will create a 3rd parquet file which will cast to the correct data types.

7- Create the postgresSQL (database="exact_yellow_cap", user="postgres", password="3690", host="127.0.0.1", port="5432") and Table named ("ny_yc_trip").

8- Run the api.py file that will start a flask app API and use the link generated in the terminal at  http://127.0.0.1:5000/ and add the extensions you want.
    This will return a json response of the filtered data and display them in the browser.
    
9- If you want to write these data to a postgresSQL database - Run the "ny_yc_write_to_db.py" file and wait for it to establish a connection and finish the process

   NOTE: This will take some time as it is inserting 16.3 mil records
   
   
   
