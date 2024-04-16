import streamlit as st
import mysql.connector
import googleapiclient.discovery
import pandas as pd
import isodate
mydb = mysql.connector.connect(host="localhost",user="root",password="")
mycursor = mydb.cursor(buffered=True)
try:
    mycursor.execute("create database capstone2")
    mydb.commit()
    mycursor.execute("use capstone2")
    mycursor.execute("create table data1(channel_id varchar(255) primary key,channel_name varchar(255),playlist_id varchar(255),total_views int(10),total_videos int(10),subscribers int(10))")
    mydb.commit()
    mycursor.execute("create table data2 (video_id varchar(255) ,comment varchar(255),author varchar(255),comment_time varchar(255))")
    mydb.commit()
    mycursor.execute("create table data3 (channel_name varchar(255),video_id varchar(255) primary key,video_title varchar(255),views int(10),likes int(10),video_date datetime,video_duration int(10),comment_count int(10))")
    mydb.commit()
except:
    pass
api="enter api key"
youtube=googleapiclient.discovery.build("youtube","v3",developerKey=api)
# function to get comment details
def c4(video_id):
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
    # create comment based dataframe
            d2=pd.DataFrame(data2)
#insert collected data to mysql
        for i in range(len(comment)):
            mycursor.execute("insert into data2(video_id,comment,author,comment_time)values(%s,%s,%s,%s)",(video_id2[i],comment[i],author[i],comment_time[i]))
            mydb.commit()
# function will get each video information
def c3(video_id):
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
    d1=pd.DataFrame(data1)
#insert collected data to mysql
    for i in range(len(video_id)):
        mycursor.execute("insert into data3(channel_name,video_id,video_title,views,likes,video_date,video_duration,comment_count)values(%s,%s,%s,%s,%s,%s,%s,%s)",(channel_name1[i],video_id[i],video_title[i],views[i],likes[i],video_date[i],v_sec[i],comment_count[i]))
    mydb.commit()
    return c4(video_id)
# function to get video id of each video
def c2(playlist_id):
    request2=youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=50).execute()
    video_id=[]
    for i in range(len(request2['items'])):
        id=request2['items'][i]['snippet']['resourceId']['videoId']
        video_id.append(id)
    return c3(video_id)
#function to get a channel playlist id and other information
def c1(a):
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
    d=pd.DataFrame(data)
#insert collected data to mysql
    mycursor.execute("use capstone2")
    mycursor.execute("insert into data1(channel_id,channel_name,playlist_id,total_views,total_videos,subscribers)values(%s,%s,%s,%s,%s,%s)",(channel_id,channel_name,playlist_id,total_views,total_videos,subscribers))
    mydb.commit()
    return c2(playlist_id)
st.title("You Tube Data Harvesting ")
a=st.text_input("enter a channel id")
if st.button("submit"):
    try:
        r=c1(a)
    except:
        st.write("channel details already stored enter a new channel id")
mycursor.execute("use capstone2")
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
print(df9)
#creating dataframe for question 10
mycursor.execute("select channel_name,video_title,comment_count from data3 where comment_count=(select MAX(comment_count) from data3)")
ans10=mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
df10 = pd.DataFrame(ans10, columns=columns)
# storing question as key and values as answer in a dictionary

data={
    '1.What are the names of all the videos and their corresponding channels':df1,
    '2.Which channels have the most number of videos and how many videos do they have?':df2,'3.What are the top 10 most viewed videos and their respective channels?\
    ':df3,
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

