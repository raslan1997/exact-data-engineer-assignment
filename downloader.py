import csv
import requests
from download_ny import URLs
from download_ny import months
import pandas as pd
from contextlib import closing
from IPython.display import HTML
import numpy as np





#for each url in the download_ny.py file it will traverse the list and retrieve the url
#initialize a get request to download the data
#write the contents in an empty csv file called csv_file and then rename it

for i in range (0, len(URLs)):
    req = requests.get(URLs[i])
    url_content = req.content
    csv_file = open('yellow_tripdata_2020-'+months[i]+'.csv', 'wb')

    csv_file.write(url_content)
    csv_file.close()