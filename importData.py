from py2neo import Graph
 
def loadneonodes():
    graph = Graph(host='localhost', port=7687, password="1234")
#delete all the existing nodes
    graph.delete_all()
 
#Read the csv file to load the CIty nodes
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///songs.csv' AS s 
    MERGE (t:tracks{t_name:s.Song,r_year:toInteger(s.Year),rating:toInteger(s.Rating),genre:s.Genre,album_name:s.Album,duration:s.Duration})
 
    """
    graph.run(query)
#----------------------------------------------------------------------------------------------------------
#Creating nodes for all artists from a csv file
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///songs.csv' AS s MERGE (a:artists {a_name:s.Artist})
 
    """
    graph.run(query)
 
#------------------------------------------------------------------------------------------------------------
#Creating nodes for all Playlists from a csv file
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///songs.csv' AS s MERGE (p:playlists {p_name:s.Playlist})
 
    """
    graph.run(query)
    
#----------------------------------------------------------------------------------------------------------------
#Creating relation between the artists and the tracks
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///songs.csv' AS s MERGE (t:tracks{t_name:s.Song,r_year:toInteger(s.Year),rating:toInteger(s.Rating),genre:s.Genre,album_name:s.Album,duration:s.Duration})
    MERGE (a:artists{a_name:s.Artist})
    merge (t)-[:sungBy]->(a)
 
    """
    graph.run(query)
 
#-----------------------------------------------------------------------------------------------------------------
#Creating relation between the songs and the Playlists
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///songs.csv' AS s MERGE (t:tracks{t_name:s.Song,r_year:toInteger(s.Year),rating:toInteger(s.Rating),genre:s.Genre,album_name:s.Album,duration:s.Duration})
    MERGE (p:playlists{p_name:s.Playlist})
    merge (t)-[:belongsTo]->(p)
 
    """
    graph.run(query)
 
# start 
if __name__ == '__main__':
    loadneonodes()
 