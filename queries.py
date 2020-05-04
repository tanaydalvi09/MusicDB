from pymongo import MongoClient
from getpass import getpass
from py2neo import Graph
import pandas
import redis
import secrets

try: 
    conn = MongoClient() 
    print("Connected successfully!!!") 
except:   
    print("Could not connect to MongoDB") 

r = redis.Redis(host='127.0.0.1',port=6379, db=0)

#connection to neo4j database
graph = Graph(host='localhost', port=7687, password="1234")


print("Enter which option you want to choose: ")
print("New Customer? Please type Yes ")
print("Do you want to login? Login ")

new_user = input("Enter your option here: ")


def new_regis():
    db = conn.Music 
    collection = db.new_login
    global new_User_Name
    global new_Password

    new_User_Name = input("Enter Your User_Name: ")
    new_Password = input("Enter Your Password: ")
    new_First_Name = input("Enter Your First_Name: ")
    new_Last_Name = input("Enter Your Last_Name: ")
    new_Email_id =input("Enter Your Email Address: ")
    new_Phone_Number =input("Enter Your Phone_Number: ")
    new_Date_of_Birth =input("Enter Your Date_of_Birth: ")
    new_Place_of_Birth =input("Enter Your Place_of_Birth: ")

    referral_code = input("Do you have referal code? If yes please type Yes:") 
    
    if  referral_code == "Yes":
        db = conn.Music 
        collection_new = db.new_login
        collection_rew = db.rewards
        refer_id = input("Enter refer code:")
        data = collection_rew.find_one({"referred_with_code" : refer_id})
        print(data)
        referred_by_usr = input("Enter the user name who referred you?")
        if data['user_name'] == referred_by_usr:
            print("VERIFIED!!! ")
            collection_new.insert_many(
    [
        {
            "new_UserName" : new_User_Name,
            "new_Password" : new_Password,
            "new_First_Name" : new_First_Name, 
            "new_Last_Name" : new_Last_Name,
            "new_Email_id" : new_Email_id,
            "new_Phone_Number" : new_Phone_Number,
            "new_Date_of_Birth" : new_Date_of_Birth,
            "new_Place_of_Birth" : new_Place_of_Birth,  
            },
            ]
            )
            db = conn.Music     
            collection = db.rewards
            document1 = {
            "user_name":new_User_Name,
            "cash_earned":50,
            "referred_by_code": refer_id
        }
            collection.insert_one(document1)
            print("VERIFIED!!! and also earned cashpoints ")
           
        
        else:
            print("wrong user name")
    
    else:

        db.new_login.insert_many(
        [
        {
            "new_UserName" : new_User_Name,
            "new_Password" : new_Password,
            "new_First_Name" : new_First_Name, 
            "new_Last_Name" : new_Last_Name,
            "new_Email_id" : new_Email_id,
            "new_Phone_Number" : new_Phone_Number,
            "new_Date_of_Birth" : new_Date_of_Birth,
            "new_Place_of_Birth" : new_Place_of_Birth
            },
            ]
            )
        print("You have not earned cash points but your account has been created")
    
    r.set(new_User_Name, new_Password)
    
    query = """ create(u:user{username:\"""" + new_User_Name + """\"})
    """
    graph.run(query)

    query = """ match(u:user{username:\"""" + new_User_Name + """\"})
    create(p:playlists{p_name:"For You"})
    merge(u)-[x:has]->(p)
    """
    graph.run(query)
#-------------------------------------------------------------------------------------------------------------
def top_song():
    artist_name = input("Enter which artist songs you want to listen: ")

    query = """ match(a:tracks)-[x:sungBy]->(b:artists{a_name:\"""" + artist_name + """\"})
    where a.rating >= 3
    return a.t_name
    """
    result = graph.run(query).data()

    songs = []

    for x in result:
        songs.append(x['a.t_name'])
    songs = list(set(songs))
    print(songs)
   
#-------------------------------------------------------------------------------------------------------------            
login_User_Name = ""
ur_id = ""
def login_opt():
    db = conn.Music 
    collection_old = db.new_login
    global login_User_Name
    global ur_id  

    login_User_Name = input("login_Enter Your User_Name: ")
    login_Password = input("login_Enter Your Password: ")

    data = collection_old.find_one({"new_UserName" : login_User_Name})
    ur_id = data["_id"]

    user = r.keys(pattern=login_User_Name)
    if not user:
        print("user does'nt exists")
    else:   
        Password = r.get(login_User_Name)
        if Password.decode("utf-8") == login_Password:
            print("login successful")
            print("1.Enter this option if you want to listen the top songs from different artists")
            print("2.Enter this option if you want to recommend a song")
            print("3.Refer to your friend and earn some cash points")
            print("4.Search")
            print("5.Create your own playlist")
            print("6.Search a friend")
            print("7.Check 'For You' Playlist")
            print("8.Total cash earned:")
            opt = input("Enter which option you want to choose:")
            if opt ==  "1":
                top_song()
            elif opt== "2":
                getuserdata()
            elif opt== "3":
                reward_code()
            elif opt== "4":
                search()
            elif opt== "5":
                create_playlist()
            elif opt== "6":
                search_friend()
            elif opt== "7":
                for_you()
            elif opt== "8":
                total_cash()      
            else:
                print("wrong entry")
        else:
            print("wrong credentials")
         
#---------------------------------------------------------------------------------------------------------------         
def reward_code():
    random_code = secrets.token_hex(3)
    
    print(random_code)
    db = conn.Music     
    collection = db.rewards
    
    document1 = {
        "user_name":login_User_Name,
        "cash_earned":100,
        "referred_with_code": random_code
        }
    collection.insert_one(document1)
    print("referred sucessfully")

#---------------------------------------------------------------------------------------------------------------
def total_cash():
    db = conn.Music 
    collection = db.rewards
    pipeline = [{"$match":{"user_name": login_User_Name}},{"$group":{"_id":None , "sum":{"$sum":"$cash_earned"}}},
                {"$project":{"_id":0 ,"sum":1}}
                ]
    docu = collection.aggregate(pipeline)
    for x in docu:
        print (x['sum'])
    print(login_User_Name)

#---------------------------------------------------------------------------------------------------------------
def for_you():
#list of tracks which user likes
    query = """ match (a:user{username:\"""" + login_User_Name + """\"})
    match(b:tracks)
    match (a)-[:likes]->(b)
    return b.genre
    """

    result = graph.run(query).data()

#variable to store the list of genre
    genre = []

    for x in result:
        genre.append(x["b.genre"])

    genre = list(set(genre))

#variable to store the list of songs
    songs =[]
#lsit of songs which user's friends like of the same genre
    for x in genre:
        gen = x.split(',')
        for g in gen:
            query = """ match (u:user{username:\"""" + login_User_Name + """\"})-[:follows]->(a:user)-[:likes]->(t:tracks)
            where \"""" + g.strip() + """\" in split(replace(t.genre,' ',''),',')
            return t.t_name
            """
            result = graph.run(query).data()
    
            for y in result :
                songs.append(y["t.t_name"])

    songs = list(set(songs))
    print(songs)

#Checking for the 'For You' playlist and creating a relation between playlist and song
    for z in songs:
        query = """match (u:user {username:\"""" + login_User_Name + """\"})-[:has]->(p:playlists{p_name:"For You"}), (s:tracks)
        where s.t_name = \"""" + z + """\"
        merge (s)-[:belongsTo]->(p)
        """
    
        result = graph.run(query).data()    

#--------------------------------------------------------------------------------------------------------------
def search_friend():
    print("Please enter the username of the friend :")
    username = input("")

    query = """match(u:user{username:\"""" + username + """\"})
    return u.username
    """
    result = graph.run(query)
    
    for username in result:
        print("user found")
        print("do you want to follow the user?")
        follow = input("")

        if follow == "yes":
            query = """ match(u:user{username:\"""" + username['u.username'] + """\"})
            match(a:user{username:\"""" + login_User_Name + """\"})
            merge (a)-[x:follows]->(u)
            """
            graph.run(query)
            print("user followed :)")
        elif follow == "no":
            print("enjoy the music")


#--------------------------------------------------------------------------------------------------------------
def create_playlist():
    print("please enter playlist name : ")
    playlist_name = input("")

    query = """match(u:user{username:\"""" + login_User_Name + """\"})
    merge(p:playlists{p_name:\"""" + playlist_name + """\"})
    merge(u)-[:has]->(p)
    """
    graph.run(query)
    print("Please type a song you want to add in your playlist : ")
    song_name = input("")

    query = """match(a:tracks{t_name:\""""+ song_name + """\"})
    return a.t_name
    """
    
    result = graph.run(query).data()

    for song_name in result:
        query = """match(q:playlists{p_name:\"""" + playlist_name + """\"}) 
        match(s:tracks{t_name:\"""" + song_name['a.t_name'] + """\"}) 
        merge(s)-[:belongsTo]->(q)
        """
        graph.run(query)
        print("song added successfully :)")

#--------------------------------------------------------------------------------------------------------------
def search():
    print("Please enter what you want to search")
    s = input("")

    query = """match(a:tracks{t_name:\""""+ s + """\"})
    return a
    """
    result = graph.run(query).data()
    print(result)

    query = """match(a:tracks{t_name:\""""+ s + """\"})-[x:sungBy]->(b:artists)
    return b.a_name
    """
    result2 = graph.run(query).data()
    print(result2)

    query = """match(a:tracks{t_name:\""""+ s + """\"})
    return a.t_name
    """
    result3 = graph.run(query).data()


    
    for s in result3:
        print("Do you want to like this song")
        like = input("")

        if like == "yes":
            query = """ match(u:user{username:\"""" + login_User_Name + """\"})
            match(s:tracks{t_name:\"""" + s['a.t_name'] + """\"})
            merge(u)-[h:likes]->(s)
            """
            graph.run(query)
            print("song liked")
        elif like == "no":
            print("enjoy the music")    
    
#--------------------------------------------------------------------------------------------------------------   
def getuserdata():

#connection to neo4j database
    graph = Graph(host='localhost', port=7687, password="1234")

#Asking for recommendation
    print("Do you want to recommend a song to your friend?")
    user = input("")

#list of friends user follows
    if user == "yes":
        query = """
        match(u:user{username:\"""" + login_User_Name + """\"})-[x:follows]->(a:user)
        return a.username
        """
        result = graph.run(query).data()

#variable for storing user's friends data
        user_name = []

        for x in result:
            user_name.append(x["a.username"])
        user_name = list(set(user_name))
        print(user_name)
    
#selecting a friend from the list
        print("Choose a friend you want to recommend a song")
        friend = input("")
    
#list of songs user likes
        if friend in user_name:
            print("Choose a song that you want to recommend")
            query = """
            match(u:user{username:\"""" + login_User_Name + """\"})-[c:likes]->(t:tracks)
            return t.t_name
            """
            result = graph.run(query).data()

#variable to store list of songs
            songs = []
            for y in result:
                songs.append(y["t.t_name"])
            songs = list(set(songs))
            print(songs)

#selecting a song to recommend
            print("select a song")
            song = input("")

            if song in songs:
                query = """
                match(b:user{username: \"""" + friend + """\"}),(s:tracks)
                where s.t_name = \"""" + song + """\"
                merge (b) -[x:recommended {recommendedBy:\"""" + login_User_Name + """\"}]-> (s)
                """
                result = graph.run(query)
                print("Song recommended")
        
        else: 
            print("user not found")

#list of songs user has recommended
    elif user == "no":
        print("Do you want to see which songs you recommended?")
        recommend = input("")

#list of friends of user
        if recommend == "yes":
            print("Select a user")
            query = """
            match(u:user{username:\"""" + login_User_Name + """\"})-[x:follows]->(a:user)
            return a.username
            """
            result = graph.run(query).data()

#variable to store list of user's friends
            u_name = []

            for z in result:
                u_name.append(z["a.username"])
            u_name = list(set(u_name))
            print(u_name)

            user = input("")
#songs recommended by the user
            if user in u_name:
                query = """
                match (u:user{username:\"""" + user + """\"}) -[x:recommended]-> (s:tracks)
                where x.recommendedBy = \"""" + login_User_Name + """\"
                return s.t_name
                """
                result = graph.run(query).data()

#variable to store the songs
                tracks = []

                for x in result:
                    tracks.append(x["s.t_name"])
                tracks = list(set(tracks))
                print(tracks)

#song recommended to which users
        elif recommend == "no":
            print("Do you want to see a particular song recommended to which users")
            users = input("")

#list of tracks user likes
            if users == "yes":
                print("select a song")
                query = """
                match(u:user{username:\"""" + login_User_Name + """\"})-[c:likes]->(t:tracks)
                return t.t_name
                """
                result = graph.run(query).data()
#variable to store tracks
                track = []

                for x in result:
                    track.append(x["t.t_name"])
                track = list(set(track))
                print(track)

                tr = input("")

#track recommended to which users
                if tr in track:
                    query = """
                    match (u:user) -[x:recommended]-> (s:tracks{t_name:\"""" + tr + """\"})
                    where x.recommendedBy = \"""" + login_User_Name + """\"
                    return u.username
                    """
                    result = graph.run(query).data()

#variable to store the list of users
                    user_data = []

                    for y in result:
                        user_data.append(y["u.username"])
                    user_data = list(set(user_data))
                    print("This song is recommended to :", user_data)
        
            elif users == "no":
                print("enjoy the music")
 
#-----------------------------------------------------------------------------------------------------------------                      
if new_user == "Yes":
    new_regis()
elif new_user == "Login":
    login_opt()
    
else:
    print("Please enter proper input")



