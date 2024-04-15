
Project title: YouTube data harvesting 
Objective: collect YouTube channel data using Google API key and storage in a SQL database table and use that data to answer questions selected by the user using streamlit. The entire process should run on streamlit.
Project work flow 
Extract data from Google client server >store the data in SQL >extract the data from SQL and convert it into a data frame using pandas >display it as a table using streamlit 
Create an my SQL database and required tables to store the data using python.
Step 1:Extraction of channel data
We used the steam lit text input function to get channel ID from the user and use that channel ID to extract the data we required. 
Using Google apiclient. discovery library we create a  to Google client discover through our api key by using certain functions like channels, playlist items, videos, comment threads we are able to extract a data of a particular channel by using the channel ID. 
•with the help of channels function list  we can extract data such as channel name, channel ID , total videos , subscribers, total views. 
•with the help of playlist items function we extract the video ID of that particular channel all videos 
•using video ID with video function list we can extract data such as video title likes comments views of each videos 
•using the comment threats function we can extract the comment of each videos and the person who committed that comment with the time and date of the comment 
Step 2 
•create an SQL data base and tables to insert the extracted values
•once extracted data or stored in the form of a variable or a list and converted into a PANDAS DATA FRAME 
•the extracted values are inserted directly to the SQL database tables 
Step 3 
•using the SQL query we extract the required data from the database and store it as a pandas dataframe
Step 4 
•we create a dictionary where the keys are the questions and values are the answers for that question
•we use stream with scroll option to insert all the dictionary key values so that the user can select the question to which we can give the answer in the form of a table. 
If you use the streamlit run file.py streamlit web page will be open where we can insert channel ID to store the required channel details in my SQL database and we can use scroll to select the questions and once we press submit  answer for that question will be displayed the form of a table where the data will be extracted from the my SQL database tables


