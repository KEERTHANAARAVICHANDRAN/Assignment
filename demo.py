chID =  "UCOe0r3qMBBTqrzAdootT_Ng"
api_ = "AIzaSyB9jDhmiUgQh-bpselV8x-2-DTMkq1m_VA"

from googleapiclient.discovery import build

def api_connect():
    api_ = "AIzaSyB9jDhmiUgQh-bpselV8x-2-DTMkq1m_VA"

    api_service_name = "youtube"
    api_version = "v3"

    youtube=build(api_service_name, api_version, developerKey = api_ )

    return youtube
youtube=api_connect()

def main(chID):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=chID,
    )
    response = request.execute()

    for i in response["items"]:
        data = dict(channel_Name=i["snippet"]["title"],
                channel_ID=i["id"],
                subscribers=i["statistics"]["subscriberCount"],
                Total_videoes=i["statistics"]["videoCount"],
                views=i["statistics"]["viewCount"],
                channel_Description=i["snippet"]["description"],
                playlist_ID=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

main("UCOe0r3qMBBTqrzAdootT_Ng")

def get_video_ids(chID):
        video_ids=[]

        next_page_token=None

        while True:
                request = youtube.playlistItems().list(
                                part="snippet,contentDetails",
                                maxResults=25,
                                playlistId="UUOe0r3qMBBTqrzAdootT_Ng",
                                pageToken=next_page_token).execute()

                for i in range (len(request["items"])):    
                       video_ids.append(request["items"][i]["snippet"]["resourceId"]["videoId"])
                next_page_token=request.get("nextPageToken")

                if next_page_token is None:
                      break
        return video_ids

video_Ids=get_video_ids("UCOe0r3qMBBTqrzAdootT_Ng")
video_Ids

def get_video_informations(video_Ids):
    video_data=[]
    for video_id in video_Ids:
                    
                request = youtube.videos().list(
                        part="snippet,ContentDetails,statistics",
                        id=video_id
                    )
                response = request.execute()
                for item in response["items"]:
                    data = dict(channel_Name=item["snippet"]["title"],
                                channel_ID=item["id"],
                                Video_Id=item["id"],
                                Title=item["snippet"]["title"],
                                Tags=item["snippet"].get("tags"),
                                Thumbnails=item["snippet"]["thumbnails"]["default"]["url"],
                                Description=item["snippet"].get("description"),
                                Published_Data=item["snippet"].get("publishedAt"),
                                Duration=item["contentDetails"]["duration"],
                                Views=item["statistics"].get("viewCount"),
                                Likes=item["statistics"].get("likeCount"),
                                Comments=item["statistics"].get("commentCount"),
                                Favourite_Count=item["statistics"].get("favouriteCount"),     
                                Definition=item["contentDetails"]["definition"],
                                Caption_Status=item["contentDetails"].get("caption")
                                )
                    video_data.append(data)
                for i in video_data:
                    print(i)  
    return video_data   

video_info=get_video_informations(video_Ids)


def get_comment_data(video_Ids):
    Comment_data=[]
    try:    
        for video_id in video_Ids:
            request = youtube.commentThreads().list(
                            part="snippet,replies",
                            videoId=video_id,
                            maxResults=50
            )
            response = request.execute()
            for item in response["items"]:
                    data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                                    Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    Published_data=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    Comment_data.append(data)
    except:
        pass
    return Comment_data

a=get_comment_data(video_Ids)

a


def get_playlist_data(chID):
        seperate_playlist_data=[]
        try:
                request = youtube.playlists().list(
                                part="snippet,contentDetails",
                                channelId="UCOe0r3qMBBTqrzAdootT_Ng",
                                maxResults=50
                )
                response = request.execute()

                for i in response['items']:
                        data=dict(Playlist_Id=i['id'],
                                Title=i['snippet']['title'],
                                Channel_Id=i['snippet']['channelId'],
                                Channel_Name=i['snippet']['channelTitle'],
                                Published_Data=i['snippet']['publishedAt'],
                                Video_Count=i['contentDetails']['itemCount'])
                        seperate_playlist_data.append(data)
        except:
              pass
        return seperate_playlist_data         
               
playlist_data=get_playlist_data("UCOe0r3qMBBTqrzAdootT_Ng")

playlist_data

# mongodb:
import pymongo

client=pymongo.MongoClient("mongodb://localhost:27017/?directConnection=true")
db=client["Youtube_datas"]

def channel_details(chID):
    ch_details=main(chID)
    video_ids=get_video_ids(chID)
    video_details=get_video_informations(video_Ids)
    comment_details=get_comment_data(video_Ids)
    playlist_details=get_playlist_data(chID)


    collection_1=db["channel_details"]
    collection_1.insert_one({"channel_info":ch_details,"video_info":video_details,"comment_info":comment_details,"playlist_info":playlist_details})

    return "uploaded successfully"

load=channel_details("UCOe0r3qMBBTqrzAdootT_Ng")

# sql connections:
import mysql.connector
import pandas as pd

def create_tables(user_input):
    
    mydb=mysql.connector.connect(host='localhost',
                                user='root',
                                password='Keerthanaa9799',
                                database='youtube_datas',
                                port='3306')
    cursor=mydb.cursor()
    

    
    create_query='''create table if not exists channels(channel_Name varchar(100),
                                                        channel_ID varchar(80) primary key,
                                                        subscribers bigint,
                                                        views bigint,
                                                        Total_videoes int,
                                                        channel_Description text,
                                                        playlist_ID varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()
    
    separate_channels=[]
    db=client["Youtube_datas"]
    collection_1=db["channel_details"]
    for channel_data in collection_1.find({"channel_info.channel_Name":user_input },{"_id":0}):
            separate_channels.append((channel_data["channel_info"]))

    df_separate_channels=pd.DataFrame(separate_channels)


    for index,row in df_separate_channels.iterrows():
        insert_query="""insert into channels(channel_Name,
                                            channel_ID,
                                            subscribers,
                                            Total_videoes,
                                            views,
                                            channel_Description,
                                            playlist_ID)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)"""
        values=(row["channel_Name"],
                row["channel_ID"],
                row["subscribers"],
                row["Total_videoes"],
                row["views"],
                row["channel_Description"],
                row["playlist_ID"])
        

        try:
            cursor.execute(insert_query,values) 
            mydb.commit()

        except:
            print("inserted")

create_tables("good luck")


def playlists_table(user_input):
    mydb=mysql.connector.connect(host='localhost',
                                    user='root',
                                    password='Keerthanaa9799',
                                    database='youtube_datas',
                                    port='3306')
    cursor=mydb.cursor()

    
    create_query='''create table if not exists playlists(Playlist_Id varchar(100),
                                                                        Title varchar(100),
                                                                        Channel_Id varchar(100),
                                                                        Channel_Name varchar(100),
                                                                        Published_Data text,
                                                                        Video_Count int)'''

    cursor.execute(create_query)
    mydb.commit()
    

    separate_playlist=[]
    db=client["Youtube_datas"]
    collection_1=db["channel_details"]
    for channel_data in collection_1.find({"channel_info.channel_Name": user_input },{"_id":0}):
            separate_playlist.append(channel_data["playlist_info"])

    df1_separate_playlist= pd.DataFrame(separate_playlist[0])        
  

    for index,row in df1_separate_playlist .iterrows():
            insert_query="""insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                Published_Data,
                                                Video_Count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)"""
            
            values=(row["Playlist_Id"],
                    row["Title"],
                    row["Channel_Id"],
                    row["Channel_Name"],
                    row["Published_Data"],
                    row["Video_Count"])
            
            cursor.execute(insert_query,values) 
            mydb.commit()
            print("Data got inserted successfully")
            

playlists_table("good luck")


def videos_table(user_input):
        mydb=mysql.connector.connect(host='localhost',
                                                user='root',
                                                password='Keerthanaa9799',
                                                database='youtube_datas',
                                                port='3306')
        cursor=mydb.cursor()
        
        create_query='''create table if not exists videos(channel_Name varchar(100) primary key,
                                                        channel_ID varchar(80),
                                                        Video_Id   varchar(50),
                                                        Title varchar(200),
                                                        Thumbnails varchar(200),
                                                        Description text,
                                                        Published_Data text,
                                                        Duration text,
                                                        Views bigint ,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favourite_Count int,     
                                                        Definition varchar(100),
                                                        Caption_Status varchar(100))'''
                                                
        cursor.execute(create_query)
        mydb.commit()
        

        separate_video=[]
        db=client["Youtube_datas"]
        collection_1=db["channel_details"]
        for channel_data in collection_1.find({"channel_info.channel_Name": user_input },{"_id":0}):
                separate_video.append(channel_data["video_info"])

        df2_separate_video= pd.DataFrame(separate_video[0])


        for index,row in df2_separate_video.iterrows():
                                insert_query="""insert into videos(channel_Name,
                                                                        channel_ID, 
                                                                        Video_Id,
                                                                        Title, 
                                                                        Thumbnails, 
                                                                        Description,
                                                                        Published_Data, 
                                                                        Duration, 
                                                                        Views,
                                                                        Likes, 
                                                                        Comments,
                                                                        Favourite_Count,     
                                                                        Definition,
                                                                        Caption_Status)
                                                                        
                                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                                
                                values=(row["channel_Name"],
                                        row["channel_ID"],
                                        row["Video_Id"],
                                        row["Title"],
                                        row["Thumbnails"],
                                        row["Description"],
                                        row["Published_Data"],
                                        row["Duration"],
                                        row["Views"],
                                        row["Likes"],
                                        row["Comments"],
                                        row["Favourite_Count"],
                                        row["Definition"],
                                        row["Caption_Status"])
                                
                                try:
                                        cursor.execute(insert_query,values) 
                                        mydb.commit()
                                except:    
                                       print("data got inserted successfully") 

                                       

videos_table("good luck")


def comments_table(user_input):
    mydb=mysql.connector.connect(host='localhost',
                                user='root',
                                password='Keerthanaa9799',
                                database='youtube_datas',
                                port='3306')
    cursor=mydb.cursor()
    
    create_query='''create table if not exists comments(Comment_Id varchar(100),
                                                            Video_id varchar(50),
                                                            Comment_Text   varchar(150),
                                                            Comment_Author varchar(50),
                                                            Published_data text)'''
                                                    
                                                    
    cursor.execute(create_query)
    mydb.commit()

    

    separate_comments=[]
    db=client["Youtube_datas"]
    collection_1=db["channel_details"]
    for channel_data in collection_1.find({"channel_info.channel_Name": user_input },{"_id":0}):
        separate_comments.append(channel_data["comment_info"])

    df3_separate_comments= pd.DataFrame(separate_comments[0])


    for index,row in df3_separate_comments.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                        Video_id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Published_data)
                                                        
                                                        values(%s,%s,%s,%s,%s)'''
                values=(row["Comment_Id"],
                        row["Video_id"],
                        row["Comment_Text"][0:50],
                        row["Comment_Author"],
                        row["Published_data"])
                
                cursor.execute(insert_query,values) 
                mydb.commit()
                print("data got inserted")


comments_table("good luck")


def Tables():
    create_tables()
    playlists_table()
    videos_table()
    comments_table()

    return "Tables created"


import streamlit as st


def channels_table_up():  
    list_of_channels=[]
    db=client["Youtube_datas"]
    collection_1=db["channel_details"]
    for channel_data in collection_1.find({},{"_id":0,"channel_info":1,}):
            list_of_channels.append(channel_data["channel_info"])
    df=st.dataframe(list_of_channels)  

def playlists_table_up():
    list_of_playlist=[]
    db=client["Youtube_datas"]
    collection_1=db["channel_details"]
    for playlist_data in collection_1.find({},{"_id":0,"playlist_info":1,}):
        for i in range(len(playlist_data["playlist_info"])):
            list_of_playlist.append(playlist_data["playlist_info"][i])
    df1=st.dataframe(list_of_playlist)

    return df1

def videos_table_up():
    list_of_videos=[]
    db=client["Youtube_datas"]
    collection_1=db["channel_details"]
    for videos_data in collection_1.find({},{"_id":0,"video_info":1,}):
            for i in range(len(videos_data["video_info"])):
                list_of_videos.append(videos_data["video_info"][i])
    df2=st.dataframe(list_of_videos)

    return df2

def comments_table_up():
    list_of_comments=[]
    db=client["Youtube_datas"]
    collection_1=db["channel_details"]
    for comments_data in collection_1.find({},{"_id":0,"comment_info":1,}):
        for i in range(len(comments_data["comment_info"])):
            list_of_comments.append(comments_data["comment_info"][i])
    df3=st.dataframe(list_of_comments) 

    return df3


with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING USING SQL,MONGODB AND STREAMLIT]")
    st.header("skill take away")
    st.caption("python")
    st.caption("collections of datas")
    st.caption("mongodb")
    st.caption("api integeration")
    st.caption("data using maongodb and sql")

channel_id=st.text_input("Enter the Channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_datas"]
    collection_1=db["channel_details"]
    for ch_data in collection_1.find({},{"_id":0,"channel_info":1}):
        ch_ids.append(ch_data["channel_info"]["channel_ID"])
    if channel_id in ch_ids:
        st.success("datas are already exist")   
    else:
        insert=channel_details(channel_id)
        st.success(insert)


list_of_channels=[]
db=client["Youtube_datas"]
collection_1=db["channel_details"]
for channel_data in collection_1.find({},{"_id":0,"channel_info":1}):
        list_of_channels.append(channel_data["channel_info"]["channel_Name"])         

channels=st.selectbox("select the channel",list_of_channels)            

if st.button("move to sql"):
    Table= Tables(channels)
    st.success(Table)

show_table=st.radio("view the table",("Channels","Playlists","Videos","Comments"))

if show_table=="Channels":
    channels_table_up()

elif show_table=="Playlists":
    playlists_table_up()

elif show_table=="Videos":
    videos_table_up()

elif show_table=="Comments":
    comments_table_up()



mydb=mysql.connector.connect(host='localhost',
                            user='root',
                            password='Keerthanaa9799',
                            database='youtube_datas',
                            port='3306')
cursor=mydb.cursor()

question=st.selectbox("select the question",("1.What are the names of all the videos and their corresponding channels?",
                                            "2.Which channels have the most number of videos, and how many videos do they have?",
                                            "3.What are the top 10 most viewed videos and their respective channels?",
                                            "4.How many comments were made on each video and what are their corresponding video names?",
                                            "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                            "6.what is the total number of likes and dislikes for each video, and what are their corresponding video names?", 
                                            "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                            "8.What are the names of all the channels that have published videos in the year 2023?",
                                            "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                            "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))
          


if question=="1.What are the names of all the videos and their corresponding channels?":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    table1=cursor.fetchall()
    df1=pd.DataFrame(table1,columns=["videostitle","channelname"])
    st.write(df1)

elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname,total_videoes as no_videos from channels order by total_videoes desc'''
    cursor.execute(query2)
    table2=cursor.fetchall()
    df2=pd.DataFrame(table2,columns=["channel name","no_of_videos"])
    st.write(df2)

elif question=="3.What are the top 10 most viewed videos and their respective channels?":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos where views is not null order by views desc limit 15'''
    cursor.execute(query3)
    table3=cursor.fetchall()
    df3=pd.DataFrame(table3,columns=["views","channelname","videotitle"])
    st.write(df3)    

elif question=="4.How many comments were made on each video and what are their corresponding video names?":
    query4='''select comments as no.of.comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    table4=cursor.fetchall()
    df4=pd.DataFrame(table4,columns=["no.of.comments","videotitle"])
    st.write(df4)
    
elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    table5=cursor.fetchall()
    df5=pd.DataFrame(table5,columns=["videostitle","channelname","likes"])
    st.write(df5)

elif question=="6.what is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    table6=cursor.fetchall()
    df6=pd.DataFrame(table6,columns=["likes","videotitle"])
    st.write(df6)    

elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select channel_name as channelname,views as totalviews from channels'''
    cursor.execute(query7)
    table7=cursor.fetchall()
    df7=pd.DataFrame(table7,columns=["channelname","totalviews"])
    st.write(df7)    

elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8='''select title as video_title,published_data as videorelease,channel_name as channelname from videos where extract(year from published_data)=2023'''
    cursor.execute(query8)
    table8=cursor.fetchall()
    df8=pd.DataFrame(table8,columns=["videotitle","published_data","channelname"])
    st.write(df8)

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    table9=cursor.fetchall()
    df9=pd.DataFrame(table9,columns=["channelname","averageduration"])
    table9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"],
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        table9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(table9)
    st.write(df1)

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select title as videotitle,channel_name as channelname,comments as comments from videos where comments is not null order by comments desc'''
    cursor.execute(query10)
    table10=cursor.fetchall()
    df10=pd.DataFrame(table10,columns=["videotitle","channelname","comments"])
    st.write(df10)       