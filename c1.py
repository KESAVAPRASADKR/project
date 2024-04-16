import pandas as pd
import streamlit as st
import mysql.connector
mydb = mysql.connector.connect(host="localhost",user="root",password="")
mycursor = mydb.cursor(buffered=True)
try:
    #creating the mysql database
    mycursor.execute("create database capstone1")
    mydb.commit()
    mycursor.execute("use capstone1")
    mycursor.execute("create table data1(channel_id varchar(255) primary key,channel_name varchar(255),playlist_id varchar(255),total_views int(10),total_videos int(10),subscribers int(10))")
    mydb.commit()
    mycursor.execute("create table data2 (video_id varchar(255) ,comment varchar(255),author varchar(255),comment_time varchar(255))")
    mydb.commit()
    mycursor.execute("create table data3 (channel_name varchar(255),video_id varchar(255) primary key,video_title varchar(255),views int(10),likes int(10),video_date datetime,video_duration int(10),comment_count int(10))")
    mydb.commit()
except:
    pass
#connecting to mysql database
mydb = mysql.connector.connect(host="localhost",user="root",password="",database="capstone1")
mycursor = mydb.cursor(buffered=True)
import googleapiclient.discovery
import isodate
from sqlalchemy import create_engine
username = 'root'
password = ''
host = 'localhost'  
database_name = 'capstone1'
engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}/{database_name}')
api="enter your apikey"
youtube=googleapiclient.discovery.build("youtube","v3",developerKey=api)
#to get channel details
def c4(video_id):# to get comment details
    for i in video_id:
        request5 = youtube.commentThreads().list(
            part="snippet",
            videoId=i,
            maxResults=100
        )
        response5 = request5.execute()
        video_id2=[]
        comment=[]
        author=[]
        comment_time=[]
        for i in range(len(response5["items"])):
            c=response5["items"][i]['snippet']['topLevelComment']['snippet']['textOriginal']
            a=response5["items"][i]['snippet']['topLevelComment']['snippet']['authorDisplayName']
            c_d=response5["items"][i]['snippet']['topLevelComment']['snippet']['updatedAt']
            id=response5["items"][0]['snippet']['videoId']
            comment.append(c)
            author.append(a)
            comment_time.append(c_d)
            video_id2.append(id)   
        data2={"video_id":video_id2,"comment":comment,"author":author,"comment_time":comment_time}
        # storing the values into a pandas dataframe
        d2=pd.DataFrame(data2)
        # convert dataframe column values to mysql column datatype
        d2["video_id"]=d2["video_id"].astype(str)
        d2['comment']=d2['comment'].astype(str)
        d2['author']=d2['author'].astype(str)
        d2['comment_time']=pd.to_datetime(d2['comment_time'])
        table_name = 'data2'
        d2.to_sql(table_name, con=engine, if_exists='append', index=False)
        mydb.commit()
def c3(video_id):  
    # to get each video data such as like,views,total comment
    views=[]
    likes=[]
    comment_count=[]
    video_title=[]
    video_date=[]
    video_duration=[]
    channel_name1=[]
    for i in video_id:
        request3 = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=i
            )
        response3 = request3.execute()
        name=response3["items"][0]["snippet"]["channelTitle"]
        title=response3['items'][0]['snippet']['title']
        vc=response3['items'][0]['statistics']['viewCount']
        try:
            l=response3['items'][0]['statistics']['likeCount']
        except:
            l="none"
        try:
            cc=response3['items'][0]['statistics']['commentCount']
        except:
            cc="none"
        date=response3['items'][0]['snippet']['publishedAt']
        duration=response3['items'][0]['contentDetails']['duration']
        channel_name1.append(name)
        views.append(vc)
        video_date.append(date)
        video_duration.append(duration)
        comment_count .append(cc)
        likes.append(l)
        video_title.append(title)
        v_sec=[]
    # converting the video duration into seconds
    for i in video_duration:
        duration= isodate.parse_duration(i)
        seconds=duration.total_seconds()
        s1=int(seconds)
        v_sec.append(s1)
    # insert the extracted data to mysql database table   
    data1={"channel_name":channel_name1,"video_id":video_id,"video_title":video_title,"views":views,"likes":likes,"video_date":video_date,"video_duration":v_sec,"comment_count":comment_count}
    # storing the values into a pandas dataframe
    d1=pd.DataFrame(data1)
    # convert dataframe column values to mysql column datatype
    d1['channel_name']=d1['channel_name'].astype(str)
    d1['video_id']=d1['video_id'].astype(str)
    d1['video_title']=d1['video_title'].astype(str)
    d1['views']=d1['views'].astype(int)
    d1['likes']=d1['likes'].astype(int)
    d1['video_date']=pd.to_datetime(d1['video_date'])
    d1['video_duration']=d1['video_duration'].astype(int)
    d1['comment_count']=d1['comment_count'].astype(int)
    table_name = 'data3'
    d1.to_sql(table_name, con=engine, if_exists='append', index=False)
    mydb.commit()
    return c4(video_id)
  
def c2(playlist_id):# to get video id
    request2=youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=50).execute()
    video_id=[]
    for i in range(len(request2['items'])):
        id=request2['items'][i]['snippet']['resourceId']['videoId']
        video_id.append(id)
    return c3(video_id)
def c1(a):# to get channel details
    request = youtube.channels().list(part="snippet,contentDetails,statistics,status",id=a)
    response = request.execute()
    channel_id = response['items'][0]['id']
    channel_name = response['items'][0]['snippet']['title']
    playlist_id  = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    total_views = response['items'][0]['statistics']['viewCount']
    total_videos = response['items'][0]['statistics']['videoCount']
    subscribers = response['items'][0]['statistics']['subscriberCount']
    data={"channel_id":[channel_id],
        "channel_name":[channel_name],"playlist_id":[playlist_id],"total_views":[total_views],
        "total_videos":[total_videos],"subscribers":[subscribers]}
    # storing the values into a pandas dataframe
    d=pd.DataFrame(data)
    # convert dataframe column values to mysql column datatype
    d["channel_id"]=d['channel_id'].astype(str)
    d['channel_name']=d['channel_name'].astype(str)
    d['playlist_id']=d['playlist_id'].astype(str)
    d['total_views']=d['total_views'].astype(int)
    d['total_videos']=d['total_videos'].astype(int)
    d['subscribers']=d['subscribers'].astype(int)
    table_name = 'data1'
    d.to_sql(table_name, con=engine, if_exists='append', index=False)
    mydb.commit()
    return c2(playlist_id)
st.title("You Tube Data Harvesting")
a=st.text_input("enter a channel id")# getting channel id from user
if st.button("submit"):
    try:
        r=c1(a) #  call function to extract channel information
    except:
        st.write("channel details already stored enter a new channel id")

# getting data from sql and converting into pandas dataframe for question 1
mycursor.execute('select channel_name,video_title from data3')
ans1=mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
df1 = pd.DataFrame(ans1, columns=columns)
#creating dataframe for question 2
mycursor.execute("select channel_name,total_videos from data1 where total_videos=(select MAX(total_videos) from data1)")
ans2=mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
df2 = pd.DataFrame(ans2, columns=columns)
#creating dataframe for question 3
mycursor.execute('select channel_name,video_title,views from data3  order by views desc limit 10')
ans3=mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
df3 = pd.DataFrame(ans3, columns=columns)
#creating dataframe for question 4
mycursor.execute('select video_title,comment_count from data3')
ans4=mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
df4 = pd.DataFrame(ans4, columns=columns)
#creating dataframe for question 5
mycursor.execute("select channel_name,video_title,likes from data3 where likes=(select MAX(likes) from data3)")
ans5=mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
df5 = pd.DataFrame(ans5, columns=columns)
#creating dataframe for question 6
mycursor.execute('select video_title,likes from data3')
ans6=mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
df6 = pd.DataFrame(ans6, columns=columns)
#creating dataframe for question 7
mycursor.execute('select channel_name,total_views from data1')
ans7=mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
df7 = pd.DataFrame(ans7, columns=columns)
#creating dataframe for question 8
mycursor.execute('select distinct channel_name from data3 where year(video_date)=2022 ')
ans8=mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
df8 = pd.DataFrame(ans8, columns=columns)
#creating dataframe for question 9
mycursor.execute('select channel_name,avg(video_duration) from data3 group by channel_name')
ans9=mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
df9 = pd.DataFrame(ans9, columns=columns)
#creating dataframe for question 10
mycursor.execute("select channel_name,video_title,comment_count from data3 where comment_count=(select MAX(comment_count) from data3)")
ans10=mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
df10 = pd.DataFrame(ans10, columns=columns)
# storing question as key and values as answer in a dictionary
data={
    '1.What are the names of all the videos and their corresponding channels':df1,
    '2.Which channels have the most number of videos and how many videos do they have?':df2,
    '3.What are the top 10 most viewed videos and their respective channels?'
    :df3,
    '4.How many comments were made on each video and what are theircorresponding video names?':df4,
    '5.Which videos have the highest number of likes and what are their corresponding channel names?':df5,
    '6.What is the total number of likes for each video and   what are their corresponding video names?':df6,
    '7.What is the total number of views for each channel, and what are their corresponding channel names?':df7,
    '8.What are the names of all the channels that have published videos in the year2022?':df8,
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':df9,
    '10.which videos have highest number of comments,and what are their corresponding channel name':df10 }
st.title("**TABLE DETAILS**")
# each key value in data is added in select box which reprisent a question
question = st.selectbox("**Select a question:**", list(data.keys()))
# show answer button is added to display answer for selected question
if st.button("Show Answer"):
    answer = data[question]
    st.write(answer)
