import json
import sqlite3      

#Do NOT put functions/statement outside functions

def start():
    #import JSON into DBz
    file = 'airbnb.json'

    with open(file, 'r',encoding="utf8") as myfile:
        data=myfile.read()

    listing = json.loads(data)

    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS review")
    c.execute("DROP TABLE IF EXISTS reviewer")
    c.execute("DROP TABLE IF EXISTS amenities")
    c.execute("DROP TABLE IF EXISTS accommodation")
    c.execute("DROP TABLE IF EXISTS host_accommodation")
    c.execute("DROP TABLE IF EXISTS host")

    c.execute('''
        CREATE TABLE reviewer 
        (rid INTEGER PRIMARY KEY, rname TEXT)
        ''')

    c.execute('''
        CREATE TABLE accommodation
        (id INTEGER PRIMARY KEY, name TEXT,summary TEXT,url TEXT,review_score_value INTEGER)
        ''')

    c.execute('''
        CREATE TABLE review 
        (id INTEGER  PRIMARY KEY autoincrement,rid INTEGER,comment TEXT,datetime TEXT,accommodation_id INTEGER,
        CONSTRAINT reviewer_rid_fk_column FOREIGN KEY (rid) REFERENCES reviewer (rid),
        CONSTRAINT reviewer_accommodation_id_fk_column FOREIGN KEY (accommodation_id) REFERENCES accommodation (id))
        ''')

    c.execute('''
    	CREATE TABLE amenities 
    	(accommodation_id INTEGER ,type TEXT,
        PRIMARY KEY (accommodation_id, type),
        CONSTRAINT amenities_accommodation_id_fk_column FOREIGN KEY (accommodation_id) REFERENCES accommodation (id))
    	''')

    c.execute('''
        CREATE TABLE host
        (host_id INTEGER PRIMARY KEY, host_url TEXT, host_name TEXT, host_about TEXT, host_location TEXT)
        ''')

    c.execute('''
        CREATE TABLE host_accommodation
        (host_id INTEGER , accommodation_id INTEGER,
        PRIMARY KEY (host_id, accommodation_id),
        CONSTRAINT host_accommodation_host_id_fk_column FOREIGN KEY (host_id) REFERENCES host (host_id),
        CONSTRAINT host_accommodation_accommodation_id_fk_column FOREIGN KEY (accommodation_id) REFERENCES accommodation (id))
        ''')


    
    for i in listing:
        accommodation_id = i["_id"]
        for j in i["reviews"]:
            rid = j["reviewer_id"]
            comment = j["comments"]
            datetime = j["date"]["$date"]
            c.execute("INSERT INTO review (rid, comment,datetime,accommodation_id) VALUES (?,?,?,?)",(rid,comment,datetime,accommodation_id))


    for i in listing:
        for j in i["reviews"]:
            rid = j["reviewer_id"]
            rname = j["reviewer_name"]
            c.execute("INSERT or IGNORE INTO reviewer (rid,rname) VALUES (?,?)",(rid,rname))


    for i in listing:
        id = i["_id"]
        name = i["name"]
        summary = i["summary"]
        url = i["listing_url"]
        if len(i["review_scores"]) == 0:
            review_score_value = None
        else:
            review_score_value = i["review_scores"]["review_scores_value"] 
        c.execute("INSERT INTO accommodation (id,name,summary,url,review_score_value) VALUES (?,?,?,?,?)",(id,name,summary,url,review_score_value))

    for i in listing:
        accommodation_id = i["_id"]
        for type in list(dict.fromkeys(i["amenities"])):
            c.execute("INSERT INTO amenities (accommodation_id,type) VALUES (?,?)",(accommodation_id,type))

    for i in listing:
            host_id = i["host"]["host_id"]
            host_url = i["host"]["host_url"]
            host_name = i["host"]["host_name"]
            host_about = i["host"]["host_about"]
            host_location = i["host"]["host_location"]
            c.execute("INSERT or IGNORE INTO host (host_id,host_url,host_name,host_about,host_location) VALUES (?,?,?,?,?)", (host_id,host_url,host_name,host_about,host_location))

    for i in listing:
            host_id = i["host"]["host_id"]
            accommodation_id = i["_id"]
            c.execute("INSERT INTO host_accommodation (host_id,accommodation_id) VALUES (?,?)", (host_id,accommodation_id))

    conn.commit()
    conn.close()

if __name__ == '__main__':
    start()

