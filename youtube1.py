# All Packages:
from googleapiclient.discovery import build
import psycopg2
import pymongo
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu



# API Functions:


api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyAvNB4_ac_fPg5uHroI2-VUfzRgBtyVTG8"
channel_id = 'UCuI5XcJYynHa5k_lqDzAgwQ'

youtube = build(api_service_name, api_version, developerKey=api_key)

client = pymongo.MongoClient("mongodb+srv://pvasudwvan:pvasudwvan@cluster0.m0kpwkl.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_data"]


##Function to get channels details:

def get_channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    channel_id = response['items'][0]["id"]
    channel_name = response['items'][0]['snippet']['title']
    channel_description = response['items'][0]['snippet']['description']
    channel_pat = response['items'][0]['snippet']['publishedAt']
    channel_playlist = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    channel_scount = response['items'][0]['statistics']['subscriberCount']
    channel_vcount = response['items'][0]['statistics']['videoCount']
    channel_viewCount = response['items'][0]['statistics']['viewCount']

    d = {
        'channel_id': channel_id,
        'channel_name': channel_name,
        'channel_des': channel_description,
        'p@t': channel_pat,
        'playlist': channel_playlist,
        'subcount': channel_scount,
        'vc': channel_vcount,
        'viewCount': channel_viewCount
    }
    return d

channel_1 = get_channel_details('UCuI5XcJYynHa5k_lqDzAgwQ')
channel_2 = get_channel_details('UCNIy6zQyP7SuLEIaiwymfUA')
channel_3 = get_channel_details('UC36qgdaYNkwCgjKjgiCqxeA')
channel_4 = get_channel_details('UCBF5i6PogoMwnoAP0LFiCmQ')
channel_5 = get_channel_details('UC9kKt-14c3tjGJSO_jwPHnA')
channel_6 = get_channel_details('UClfOtvtVrQA9Msxef0s34jQ')
channel_7 = get_channel_details('UCTMJmZHXDyHrMtilKaN9J4w')
channel_8 = get_channel_details('UC0GDHStEIx9n4FqUiDkxiRg')
channel_9 = get_channel_details('UCjZC9-Ym0UNMxqgcDX4Q0dg')
channel_10 = get_channel_details('UC5fcjujOsqD-126Chn_BAuA')


# Video Id's Details Functions:

playlist_id = 'UUTMJmZHXDyHrMtilKaN9J4w'

def get_video_ids(youtube, playlist_id):
    request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=playlist_id,
        maxResults=50)
    response = request.execute()

    video_ids = []

    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    nextpagetoken = response.get('nextPageToken')
    extra_pages_is_there = True

    while extra_pages_is_there:
        if nextpagetoken is None:
            extra_pages_is_there = False

        else:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=nextpagetoken)

            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

            nextpagetoken = response.get('nextPageToken')

    return video_ids

get_video_ids(youtube, playlist_id)

video_ids = get_video_ids(youtube, playlist_id)




# Video Details Functions:


def get_video_ids(youtube, video_ids):
    all_video_stats = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,statistics",
            id=','.join(video_ids[i:i + 50]))
        response = request.execute()

        for video in response['items']:
            video_stats = dict(Title=video['snippet']['title'],
                               published_date=video['snippet']['publishedAt'],
                               Channel_des=video['snippet']['description'],
                               Views=video['statistics']['viewCount'],
                               Likes=video['statistics']['likeCount'],
                               Comments=video['statistics']['commentCount'],
                               )
            all_video_stats.append(video_stats)

    return all_video_stats

video_details = get_video_ids(youtube, video_ids)

video_data = pd.DataFrame(video_details)


# Comments Details Functions:


def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50

            )
            response = request.execute()

            for item in response['items']:
                data = dict(comment_Id=item['snippet']['topLevelComment']['id'],
                            video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])

                comment_data.append(data)
    except:
        pass
    return comment_data

comment_details = get_comment_info(video_ids)

comment_data = pd.DataFrame(comment_details)


# Mongodb functions:


def channel_details(channel_id):
    ch_details = get_channel_details(channel_id)
    vi_ids = get_video_ids(youtube, playlist_id)
    vi_details = get_video_ids(youtube, video_ids)
    com_details = get_comment_info(video_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information": ch_details})

    coll2 = db["video_details"]
    coll2.insert_one({"video information": vi_details})

    coll3 = db["comment_details"]
    coll3.insert_one({"comment information": com_details})

    return "upload completed successfully"


# Table create functions in postgre SQL:


def channels_table():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="pvasudwvan",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists channels(channel_id varchar(80) primary key,
                                                            channel_name varchar(100),
                                                            channel_description text, 
                                                            channel_pat varchar(80),
                                                            channel_playlist varchar(80),
                                                            channel_subscribercount bigint,
                                                            channel_videocount int,
                                                            channel_viewCount bigint)'''
    cursor.execute(create_query)
    mydb.commit()

    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''insert into channels(channel_id,
                                             channel_name,   
                                             channel_description,
                                             channel_pat,
                                             channel_playlist,
                                             channel_subscribercount,
                                             channel_videocount,
                                             channel_viewCount)

                                             values(%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['channel_id'],
                  row['channel_name'],
                  row['channel_des'],
                  row['p@t'],
                  row['playlist'],
                  row['subcount'],
                  row['vc'],
                  row['viewCount'])

        cursor.execute(insert_query, values)
        mydb.commit()


def videos_table():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="pvasudwvan",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists videos(Title varchar(100),
                                                       published_date timestamp,
                                                       Channel_des text,
                                                       Views bigint,
                                                       Likes int,
                                                       Comments text)'''
    cursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db = client["Youtube_data"]
    coll2 = db["video_details"]
    for vi_data in coll2.find({}, {"_id": 0, "video information": 1}):
        for i in range(len(vi_data["video information"])):
            vi_list.append(vi_data["video information"][i])
    dfm1 = pd.DataFrame(vi_list)

    for index, row in dfm1.iterrows():
        insert_query = '''insert into videos(Title,
                                           published_date,
                                           Channel_des,
                                           Views,
                                           Likes,
                                           Comments)

                                            values(%s,%s,%s,%s,%s,%s)'''

        values = (row['Title'],
                  row['published_date'],
                  row['Channel_des'],
                  row['Views'],
                  row['Likes'],
                  row['Comments'])

        cursor.execute(insert_query, values)
        mydb.commit()


def comments_table():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="pvasudwvan",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists comments(comment_id varchar(100) primary key,
                                                        video_id varchar(50), 
                                                        comment_text text,
                                                        comment_Author varchar(150),
                                                        comment_published timestamp)'''

    cursor.execute(create_query)
    mydb.commit()

    com_list = []
    db = client["Youtube_data"]
    coll3 = db["comment_details"]
    for com_data in coll3.find({}, {"_id": 0, "comment information": 1}):
        for i in range(len(com_data["comment information"])):
            com_list.append(com_data["comment information"][i])
    dfm2 = pd.DataFrame(com_list)

    for index, row in dfm2.iterrows():
        insert_query = '''insert into comments(comment_Id,
                                             video_Id,
                                             comment_Text,
                                             comment_Author,
                                             comment_Published)

                                             values(%s,%s,%s,%s,%s)'''

        values = (row['comment_Id'],
                  row['video_Id'],
                  row['comment_Text'],
                  row['comment_Author'],
                  row['comment_Published'])

        cursor.execute(insert_query, values)
        mydb.commit()


# All Tables creation functions:


def tables():
    channels_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"


# DataFrame functions to view in streamlit:


def show_channels_tables():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)

    return df


def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll2 = db["video_details"]
    for vi_data in coll2.find({}, {"_id": 0, "video information": 1}):
        for i in range(len(vi_data["video information"])):
            vi_list.append(vi_data["video information"][i])
    dfm1 = st.dataframe(vi_list)

    return dfm1


def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll3 = db["comment_details"]
    for com_data in coll3.find({}, {"_id": 0, "comment information": 1}):
        for i in range(len(com_data["comment information"])):
            com_list.append(com_data["comment information"][i])
    dfm2 = st.dataframe(com_list)

    return dfm2


# Streamlit coding:


st.header("Data collection zone and Store data to MongoDB and to SQL")


# CREATING OPTION MENU
with st.sidebar:
    st.caption = option_menu(None, ["About","Youtube Data Harvesting and Warehousing","Created by *Parthiban Vasudevan!*"],
                          icons=["exclamation-circle","toggles","card-text"],
                          default_index=0,
                          orientation="vertical",
                          styles={"nav-link": {"font-size": "15px", "text-align": "centre", "margin": "0px",
                                               "--hover-color": "#C80101"},
                                  "icon": {"font-size": "15px"},
                                  "container" : {"max-width": "2000px"},
                                  "nav-link-selected": {"background-color": "#C80101"}})


channel_id = st.text_input("Enter the channel ID")

if  st.button("Extract Data"):
    st.write('''(Note:- This zone **extracting of channel data from YouTube** .)''')
    ch_details = get_channel_details(channel_id)
    dfa = st.dataframe(ch_details)



if st.button("collect and store data"):
    st.write('''(Note:- This zone **extracting data from Youtube and inserting data into MongoDB** .)''')
    ch_ids = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])
    

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert = channel_details(channel_id)
        st.success(insert)
        dfam = st.dataframe(get_channel_details(channel_id))
        st.write("Channel details successfully updated in MongoDB")

if st.button('Migrate to Sql'):
    st.write('''(Note:- This zone **extracting data from MongoDB and inserting table into SQL** .)''')
    st.success("Channel details successfully updated in SQL")

    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="pvasudwvan",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists channels(channel_id varchar(80) primary key,
                                                            channel_name varchar(100),
                                                            channel_description text, 
                                                            channel_pat varchar(80),
                                                            channel_playlist varchar(80),
                                                            channel_subscribercount bigint,
                                                            channel_videocount int,
                                                            channel_viewCount bigint)'''
    cursor.execute(create_query)
    mydb.commit()

    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''insert into channels(channel_id,
                                             channel_name,   
                                             channel_description,
                                             channel_pat,
                                             channel_playlist,
                                             channel_subscribercount,
                                             channel_videocount,
                                             channel_viewCount)
    
                                             values(%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['channel_id'],
                  row['channel_name'],
                  row['channel_des'],
                  row['p@t'],
                  row['playlist'],
                  row['subcount'],
                  row['vc'],
                  row['viewCount'])

        cursor.execute(insert_query, values)
        mydb.commit()

    
st.header('TABLE VIEW')


show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    show_channels_tables()

elif show_table == "VIDEOS":
    show_videos_table()

elif show_table == "COMMENTS":
    show_comments_table()

# SQL connection:

st.header('Channel Data Analysis zone')
st.write ('''(Note:- This zone **Analysis of a collection of channel data** depends on your question selection and gives a table format output.)''')



mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="pvasudwvan",
                        database="youtube_data",
                        port="5432")
cursor = mydb.cursor()

question = st.selectbox("Select your question", ("1. All the videos and the channel name",
                                                 "2. Channels with most number of videos",
                                                 "3. 10 most viewed videos",
                                                 "4. Comments in each videos",
                                                 "5. Videos with highest likes",
                                                 "6. Likes of all videos",
                                                 "7. Views of each channel",
                                                 "8. Videos published in the year of 2022",
                                                 "9. Average duration of all videos in each channel",
                                                 "10. Videos with highest number comments"))

if question == "1. All the videos and the channel name":
    query1 = '''select title as videotitle,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns=["videotitle", "channelname"])
    st.write(df)

elif question == "2. Channels with most number of videos":
    query2 = '''select channel_name as channelname,channel_videocount as No_videos from channels
            order by channel_videocount desc'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channelname", "No of videos"])
    st.write(df2)

elif question == "3. 10 most viewed videos":
    query3 = '''select views as views,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views", "channelname", "videotitle"])
    st.write(df3)

elif question == "4. Comments in each videos":
    query4 = '''select comments as No_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["No of comments", "videotitle"])
    st.write(df4)

elif question == "5. Videos with highest likes":
    query5 = '''select title as videotitle,channel_name as channelname,likes as likecount 
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["videotitle", "channelname", "likecount"])
    st.write(df5)

elif question == "6. Likes of all videos":
    query6 = '''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6)

elif question == "7. Views of each channel":
    query7 = '''select channel_name as channelname,channel_viewcount as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["channelname", "totalviews"])
    st.write(df7)

elif question == "8. Videos published in the year of 2022":
    query8 = '''select title as videotile,published_date as publisheddate,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["videotile", "publisheddate", "channelname"])
    st.write(df8)


elif question == "9. Average duration of all videos in each channel":
    query9 = '''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channelname", "averageduration"])

    T9 = []
    for index, row in df9.iterrows():
        channel_title = row["channelname"]
        average_duration = row["averageduration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
    df123 = pd.DataFrame(T9)
    st.write(df123)


elif question == "10. Videos with highest number comments":
    query10 = '''select title as videotitle,channel_name as channelname,comments as comments from videos 
                where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["videotitle", "channelname", "comments"])
    st.write(df10)
