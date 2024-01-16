from http import client
from googleapiclient.discovery import build
import pymongo
from pymongo.mongo_client import MongoClient
import certifi
import psycopg2
import pandas as pd
import streamlit as st
import certifi
import base64

#API key Connection

def API_Connect():
    API_ID = "AIzaSyDueolOmK4xhhL0rFckoQFE5gqD_99my-I"

    API_Service_Name = "youtube"

    API_Version_Name =  "V3"

    Youtube = build(API_Service_Name,API_Version_Name,developerKey=API_ID)

    return Youtube


Youtube = API_Connect()
#print("You tube Build Success")



#Create Connection with Mongo DB
ca = certifi.where()

client=pymongo.MongoClient("mongodb+srv://subashtherider0:subash123@cluster0.z5bcdny.mongodb.net/?retryWrites=true&w=majority",tlsCAFile=ca)

db=client["Youtube_data"]

def Postgress_Connection():
        mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Postpass@123",
                            database="Youtube_Data",
                            port="5432")
        
        cursor = mydb.cursor()

        return cursor,mydb

#Get Channel Information
def Get_channel_Info(Channel_details_id):
    Request = Youtube.channels().list(
                    part = "snippet,ContentDetails,statistics",
                    id = Channel_details_id
    )

    Response = Request.execute() 

    for i in range(0,len(Response["items"])):
        data = dict(Channel_Name = Response["items"][i]["snippet"]["title"],
                    Channel_Id = Response["items"][i]["id"],
                    Subscriber_Count = Response["items"][i]["statistics"]["subscriberCount"],
                    View_Count = Response["items"][i]["statistics"]["viewCount"],
                    Total_Videos = Response["items"][i]["statistics"]["videoCount"],
                    Channel_Description=Response["items"][i]["snippet"]["description"],
                    Playlist_id=Response["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"])
    return data
 
#Request_channel_id=Get_channel_Info("UCngrTLZoePGDdrOmVsG4vOA")

#print(Request_channel_id)

def Get_playlist_details(Channel_Id):

    Next_Page_token=None
    playlst=[]

    while True:
	
        Request = Youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=Channel_Id,
            maxResults=50,
            pageToken=Next_Page_token
        )

        Response=Request.execute()

        for i in range(0,len(Response['items'])):
            data=dict(PlaylistId = Response['items'][i]['id'],
						Title = Response['items'][i]['snippet']['title'],
						ChannelId = Response['items'][i]['snippet']['channelId'],
						ChannelName= Response['items'][i]['snippet']['channelTitle'],
						PublishedAt = Response['items'][i]['snippet']['publishedAt'],
						VideoCount = Response['items'][i]['contentDetails']['itemCount'])
            playlst.append(data)

        Next_Page_token=Response.get('nextPageToken')

        if Next_Page_token is None:
            break
    return playlst


#Play_List_Details=Get_playlist_details("UCC5tm6G49DO303SYvhu6FKg")

#print(Play_List_Details)




def get_video_ID_details(Channel_Id):

    Videos_Id_List=[]

    Request = Youtube.channels().list(id=Channel_Id,
                                    part='contentDetails').execute()

    

    Playlist_ID = Request['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    #print("The Request playlist id = "+Playlist_ID)

    Token_pages = None

    while True:
        API_Response = Youtube.playlistItems().list(
            part="snippet",
            playlistId=Playlist_ID,
            maxResults=50,
            pageToken=Token_pages).execute()

        for i in range(len(API_Response['items'])):
            Videos_Id_List.append(API_Response['items'][i]['snippet']['resourceId']['videoId'])

        Token_pages = API_Response.get('nextPageToken')

        if Token_pages is None:
            break
    
    return Videos_Id_List

#Vid = API_Response['items'][0]['snippet']['resourceId']['videoId']

#print(len(Videos_Id_List))

   
#Vid_details = get_video_ID_details("UCngrTLZoePGDdrOmVsG4vOA")

#print(Vid_details)


def get_video_info_list(Vid_details):
    video_list_data=[]
    for i in Vid_details:
        request = Youtube.videos().list(
            part='snippet,ContentDetails,statistics',
            id = i
        )
        response = request.execute()

        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail = item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    views=item['statistics'].get('viewCount'),
                    likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_status=item['contentDetails']['caption'])
            video_list_data.append(data)
    return video_list_data

#len(video_list_data)

#video_list_data

#vid_list=get_video_info_list(Vid_details)

#print(vid_list)



# Video Comments Details
def Get_Comment_Details(Vid_details):
    comment_List=[]
    try:
        for ID in Vid_details:
            Request=Youtube.commentThreads().list(
                    part='snippet',
                    videoId=ID,
                    maxResults=50
            )

            Response = Request.execute()

            for com in Response['items']:
                    data=dict(Comment_Id=com['snippet']['topLevelComment']['id'],
                            Video_Id=com['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=com['snippet']['topLevelComment']['snippet']['textOriginal'],
                            Comment_Author=com['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_published=com['snippet']['topLevelComment']['snippet']['publishedAt']
                            )
                    comment_List.append(data)
        

    except:
        pass

    return comment_List

#Comments_data=Get_Comment_Details(Vid_details)

#print("Number of comments : ", len(Comments_data))




def channels_detail(channel_id):
    channel_detail=Get_channel_Info(channel_id)
    playlist_details=Get_playlist_details(channel_id)
    video_id_details=get_video_ID_details(channel_id)
    video_info_details=get_video_info_list(video_id_details)
    comment_details=Get_Comment_Details(video_id_details)

    coll1=db["Channel_Details"]
    coll1.insert_one({"Channel_Table":channel_detail,"Playlist_Information":playlist_details,
                      "Video_Information":video_info_details,"Comment_Information":comment_details})
    
    return "Data Upload to Mongo DB  Successfully"
    

    


#Insert = channels_detail(channel_id)


#Table Creations For Postgress Connection
def postgress_channel_table():

    cursor,mydb=Postgress_Connection()

    

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists channels(Channel_Name VarChar(100),
                                                            Channel_Id VarChar(80) primary key,
                                                            Subscriber_Count bigint,
                                                            View_Count bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_id VarChar(80))'''

            
    cursor.execute(create_query)
    mydb.commit()
    
     

    #only channel details and specify 
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["Channel_Details"]
    for ch_data in coll1.find({},{"_id":0,"Channel_Table":1}):
        print(ch_data['Channel_Table'])
        ch_list.append(ch_data['Channel_Table'])

    pdf = pd.DataFrame(ch_list)

    print(pdf)

    for index,row in pdf.iterrows():
        print("Index",index)
        print("Row",row)
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscriber_Count,
                                            View_Count,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_id)
                                            
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Subscriber_Count'],
                row['View_Count'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_id'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()    
        except:
            st.write("Channel Data are already Existed")


#postgress_channel_table()
        
def Playlist_Data_Table():
    cursor,mydb=Postgress_Connection()

    drop_query='''drop table if exists playlist'''
    cursor.execute(drop_query)
    mydb.commit()


    
    create_query='''create table if not exists playlist(PlaylistId varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int)'''

    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["Channel_Details"]
    for pl_data in coll1.find({},{"_id":0,"Playlist_Information":1}): #this for loop run in channel level
        #print(len(pl_data["Playlist_Information"]))# to get the total number of playlist in the channel
        for i in range(len(pl_data["Playlist_Information"])):
            pl_list.append(pl_data["Playlist_Information"][i])
    pldf=pd.DataFrame(pl_list)

    for index,row in pldf.iterrows():
        insert_query='''insert into playlist(PlaylistId,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count)
											values(%s,%s,%s,%s,%s,%s)'''
        values = (row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            st.write("Playlist data are already Existed")
        
        


#To Get the video details
def Get_the_video_details():

    cursor,mydb=Postgress_Connection()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()



    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(30) primary key,
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        views bigint,
                                                        likes bigint, 
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_status varchar(50))'''

    cursor.execute(create_query)
    mydb.commit()

    video_list=[]
    db=client["Youtube_data"]
    coll1=db["Channel_Details"]
    for vd_list in coll1.find({},{"_id":0,"Video_Information":1}): #this for loop run in channel level
        #print(len(pl_data["Playlist_Information"]))# to get the total number of playlist in the channel
        for i in range(len(vd_list["Video_Information"])):
            video_list.append(vd_list["Video_Information"][i])
    videodf = pd.DataFrame(video_list)

    for index,row in videodf.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                views,
                                                likes, 
                                                Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_status)
												values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['views'],
                row['likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_status'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            st.write("Video Data are already Existed")


#To get the comments details
def Comments_Table():

    cursor,mydb=Postgress_Connection()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                            Video_Id varchar(50),
                                                            Comment_Text text,
                                                            Comment_Author varchar(150),
                                                            Comment_published timestamp)'''

    cursor.execute(create_query)
    mydb.commit()

    cmt_list=[]
    db=client["Youtube_data"]
    coll1=db["Channel_Details"]
    for list in coll1.find({},{"_id":0,"Comment_Information":1}): #this for loop run in channel level
        #print(len(pl_data["Playlist_Information"]))# to get the total number of playlist in the channel
        for i in range(len(list["Comment_Information"])):
            cmt_list.append(list["Comment_Information"][i])
    cmtdf = pd.DataFrame(cmt_list)



    for index,row in cmtdf.iterrows():
        insert_query='''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_published)
                                            
                                            values(%s,%s,%s,%s,%s)'''
        values = (row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_published'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            st.write("Comments Data are Already Existed")


def pst_table():
    Comments_Table()
    Get_the_video_details()
    Playlist_Data_Table()
    postgress_channel_table()


    return "ETL Process is successed"

# Get the channel list details
def show_channel_table():
    #channel
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["Channel_Details"]
    for ch_data in coll1.find({},{"_id":0,"Channel_Table":1}):
        ch_list.append(ch_data["Channel_Table"])

    cldf = st.dataframe(ch_list)

    return cldf


def show_playlist_table():
#playlist
    play_list=[]
    db=client["Youtube_data"]
    coll1=db["Channel_Details"]
    for pl_list in coll1.find({},{"_id":0,"Playlist_Information":1}): #this for loop run in channel level
        #print(len(pl_data["Playlist_Information"]))# to get the total number of playlist in the channel
        for i in range(len(pl_list["Playlist_Information"])):
            play_list.append(pl_list["Playlist_Information"][i])
    pldf = st.dataframe(play_list)

    return pldf


def show_video_table():
#video
    video_list=[]
    db=client["Youtube_data"]
    coll1=db["Channel_Details"]
    for vd_list in coll1.find({},{"_id":0,"Video_Information":1}): #this for loop run in channel level
        #print(len(pl_data["Playlist_Information"]))# to get the total number of playlist in the channel
        for i in range(len(vd_list["Video_Information"])):
            video_list.append(vd_list["Video_Information"][i])
    videodf = st.dataframe(video_list)

    return videodf

def show_comment_table():
#comment
    cmt_list=[]
    db=client["Youtube_data"]
    coll1=db["Channel_Details"]
    for list in coll1.find({},{"_id":0,"Comment_Information":1}): #this for loop run in channel level
        #print(len(pl_data["Playlist_Information"]))# to get the total number of playlist in the channel
        for i in range(len(list["Comment_Information"])):
            cmt_list.append(list["Comment_Information"][i])
    cmtdf = st.dataframe(cmt_list)

    return cmtdf

page_be_image = f"""
<style>
[data-testid="stAppViewContainer"]> .main{{
background-color: #00FFFF;
background-size: cover;
background-position: top left;
background-repeat: no-repeat;
}}

[data-testid="stHeader"] {{
background-color: #00FFFF;
}}
</style>
"""

st.markdown(page_be_image,unsafe_allow_html=True)

st.header("Analysis for Youtube channel",divider='red')

st.warning(''':violet 
[Rules for Channel Id \n
The Channel Id should not contain any special character '!@#$%^&*()+{}[]|:;"<>,.?/~`'  \n
The Channel should not contain Single quotes and Double Quotes  \n 
If it Contains it will be rejected and details will not be fetched]
''')

chan_id = st.text_input("Enter the Channel ID")

choice1 , choice2 = st.columns(2)

with choice1:
    if st.button("Fetch Channel Data"):

        special_characters = set('!@#$%^&*()""<>,.?/~`')
        if any(char in special_characters for char in chan_id):
            st.error("This channel id is invalid channel id")

        else:  
            ch_ids=[]
            db=client["Youtube_data"]
            coll1=db["Channel_Details"]
            for chan_data in coll1.find({},{"_id":0,"Channel_Table":1}):

                ch_ids.append(chan_data["Channel_Table"]["Channel_Id"])


            if chan_id in ch_ids:
                st.error("This is a duplicate channel id . This channel details is already exits")

            else :
                insert=channels_detail(chan_id)
                st.toast(insert, icon='😍')
                

with choice2:        
    if st.button("ETL Process"):

        Display_Table=pst_table()
        st.toast(Display_Table,  icon='😍')


Tables = st.selectbox("Select one option to view the table",("                    ",
                                                                "Available Channels",
                                                                "Playlist",
                                                                "Videos",
                                                                "Comments"))

if Tables=="Available Channels":
    show_channel_table()

elif Tables=="Playlist":
    show_playlist_table()

elif Tables=="Videos":
    show_video_table()

elif Tables=="Comments":
    show_comment_table()



#SQL Connection
 
cursor,mydb=Postgress_Connection()



Questions = st.selectbox("Select your Questions",("               ",
                                                  "1.	What are the names of all the videos and their corresponding channels?",
                                                  "2.	Which channels have the most number of videos, and how many videos do they have?",
                                                  "3.	What are the top 10 most viewed videos and their respective channels?",
                                                  "4.	How many comments were made on each video, and what are their corresponding video names?",
                                                  "5.	Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                  "6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                  "7.	What is the total number of views for each channel, and what are their corresponding channel names?",
                                                  "8.	What are the names of all the channels that have published videos in the year 2022?",
                                                  "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                  "10.	Which videos have the highest number of comments, and what are their corresponding channel names?"))


if Questions == "1.	What are the names of all the videos and their corresponding channels?":
    question1 ='''select title as video,channel_name as Channel_Name from videos'''
    cursor.execute(question1)
    mydb.commit()
    ques1 = cursor.fetchall()
    df=pd.DataFrame(ques1,columns=["Video Title","Channel Name"])
    st.write(df)

elif Questions == "2.	Which channels have the most number of videos, and how many videos do they have?":
    question2 = '''select channel_name as Channel_Name,total_videos as Total_Videos from channels
                order by total_videos desc'''
    cursor.execute(question2)
    mydb.commit()
    ques2 = cursor.fetchall()
    df=pd.DataFrame(ques2,columns=["Channel Name","No of Videos"])
    st.write(df)

elif Questions == "3.	What are the top 10 most viewed videos and their respective channels?":
    question3 = '''select views as No_of_Views,channel_name as Channel_Name,title as Video_Title from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(question3)
    mydb.commit()
    ques3 = cursor.fetchall()
    df=pd.DataFrame(ques3,columns=["No_of_Views","Channel_Name","Video_Title"])
    st.write(df)

elif Questions == "4.	How many comments were made on each video, and what are their corresponding video names?":
    question4 = '''select comments as No_of_Comments,title as Video_Title from videos where comments is not null'''
    cursor.execute(question4)
    mydb.commit()
    ques4 = cursor.fetchall()
    df=pd.DataFrame(ques4,columns=["No_of_Comments","Video_Title"])
    st.write(df)

elif Questions == "5.	Which videos have the highest number of likes, and what are their corresponding channel names?":
    question5 = '''select title as Video_Title,channel_name as Channel_Name,likes as Like_Count
                from videos where likes is not null order by likes desc'''
    cursor.execute(question5)
    mydb.commit()
    ques5 = cursor.fetchall()
    df=pd.DataFrame(ques5,columns=["Video_Title","Channel_Name","Like_Count"])
    st.write(df)

elif Questions == "6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    question6 = '''select title as Video_Title,likes as Like_Count from videos'''
    cursor.execute(question6)
    mydb.commit()
    ques6 = cursor.fetchall()
    df=pd.DataFrame(ques6,columns=["Video_Title","Like_Count"])
    st.write(df)

elif Questions == "7.	What is the total number of views for each channel, and what are their corresponding channel names?":
    question7 = '''select channel_name as Channel_Name,view_count as Total_View_Count from channels
                order by view_count desc'''
    cursor.execute(question7)
    mydb.commit()
    ques7 = cursor.fetchall()
    df=pd.DataFrame(ques7,columns=["Channel_Name","Total_View_Count"])
    st.write(df)

elif Questions == "8.	What are the names of all the channels that have published videos in the year 2022?":
    question8 = '''select title as Video_Title,published_date as Video_Uploaded_Date,channel_name as Channel_Name from videos
                    where   extract(year from published_date)=2022'''
    cursor.execute(question8)
    mydb.commit()
    ques8 = cursor.fetchall()
    df=pd.DataFrame(ques8,columns=["Video_Title","Video_Uploaded_Date","Channel_Name"])
    st.write(df)

elif Questions == "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    question9 = '''select channel_name as Channel_Name,AVG(duration) as Average_Video_Duration from videos
                    group by channel_name'''
    cursor.execute(question9)
    mydb.commit()
    ques9 = cursor.fetchall()
    df=pd.DataFrame(ques9,columns=["Channel_Name","Average_Video_Duration"])

    tlis=[]
    for index,row in df.iterrows():
        Channel_Title = row["Channel_Name"]
        Average_Duration=row["Average_Video_Duration"]
        Average_Duration_str=str(Average_Duration)
        tlis.append(dict(Channel=Channel_Title,AVG_Duration=Average_Duration_str))
    fdf=pd.DataFrame(tlis)
    st.write(fdf)

elif Questions == "10.	Which videos have the highest number of comments, and what are their corresponding channel names?":
    question10 = '''select title as Video_Title,channel_name as Channel_Name,comments as Comments from videos
                    where comments is not null order by comments desc'''
    cursor.execute(question10)
    mydb.commit()
    ques10 = cursor.fetchall()
    df=pd.DataFrame(ques10,columns=["Video_Title","Channel_Name","Comments"])
    st.write(df)














