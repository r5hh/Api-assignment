import json
import sqlite3      

#Do NOT put functions/statement outside functions

def start():
    #import JSON into DB

    file = 'airbnb.json'
    with open(file, 'r',encoding="utf8") as myfile:
        data = myfile.read()
        
        listing = json.loads(data)
        
        def import_review():
            conn = sqlite3.connect("airbnb.db")
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS review")
            c.execute('''
                CREATE TABLE review
                (id INTEGER PRIMARY KEY autoincrement, rid INTEGER, comment TEXT, datetime TEXT, accommodation_id INTEGER,
                CONSTRAINT fk_column
                    FOREIGN KEY (rid) REFERENCES reviewer (rid),
                    FOREIGN KEY (accommodation_id) REFERENCES accommodation (id))''')
            
            for i in listing:
                accommodation_id = i["_id"]
                reviews = i["reviews"]
                for r in reviews:
                    rid = r["reviewer_id"]
                    comment = r["comments"]
                    datetime = r["date"]["$date"]
                    c.execute("INSERT INTO review (rid, comment, datetime, accommodation_id) VALUES (?,?,?,?)",(rid, comment, datetime, accommodation_id))
                
            conn.commit()
            conn.close()


        def import_reviewer():
            conn = sqlite3.connect("airbnb.db")
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS reviewer")
            c.execute('''
                CREATE TABLE reviewer
                (rid INTEGER, rname TEXT)''')

            outputid = []    
            for i in listing:
                reviews = i["reviews"]
                for r in reviews:
                    rid = r["reviewer_id"]
                    if rid not in outputid:
                        outputid.append(rid) 
                        rname = r["reviewer_name"]
                    c.execute("INSERT INTO reviewer (rid, rname) VALUES (?,?)",(rid, rname))
                
            conn.commit()
            conn.close()
        
        def import_amenities():
            conn = sqlite3.connect("airbnb.db")
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS amenities")
            c.execute('''
                CREATE TABLE amenities
                (accommodation_id INTEGER, type TEXT,
                PRIMARY KEY(accommodation_id, type)
                CONSTRAINT fk_column
                    FOREIGN KEY (accommodation_id) REFERENCES accommodation (id))''')
            
            for i in listing:
                accommodation_id = i["_id"]
                amenities = i["amenities"]
                for j in amenities:
                    while amenities.count(j) > 1:
                        amenities.remove(j)
                for j in amenities:
                    c.execute("INSERT INTO amenities (accommodation_id, type) VALUES (?,?)",(accommodation_id, j))
            
            conn.commit()            
            conn.close()

        def import_accommodation():
            conn = sqlite3.connect("airbnb.db")
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS accommodation")
            c.execute('''
                CREATE TABLE accommodation
                (id INTEGER PRIMARY KEY autoincrement, name TEXT, summary TEXT, url TWXT, review_score_value INTEGER) 
                ''')
            
            for i in listing:
                accommodation_id = i["_id"]
                name = i["name"]
                summary = i["summary"]
                url = i["listing_url"]
                if i["review_scores"]:
                    review_score_value = i["review_scores"]["review_scores_value"]
                else:
                    review_score_value = "NULL"
                c.execute("INSERT INTO accommodation (id, name, summary, url, review_score_value) VALUES (?,?,?,?,?)",(accommodation_id, name,summary, url, review_score_value))
            
            conn.commit()
            conn.close()

        def import_host_accommodation():
            conn = sqlite3.connect("airbnb.db")
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS host_accommodation")
            c.execute('''
                CREATE TABLE host_accommodation
                (host_id INTEGER, accommodation_id INTEGER, 
                PRIMARY KEY(host_id, accommodation_id)
                CONSTRAINT fk_column
                    FOREIGN KEY (host_id) REFERENCES host(host_id),
                    FOREIGN KEY (accommodation_id) REFERENCES accommodation (id))''')
  
            for i in listing:
                host_id = i["host"]["host_id"]
                accommodation_id = i["_id"]
                c.execute("INSERT INTO host_accommodation (host_id, accommodation_id) VALUES (?,?)",(host_id, accommodation_id))
                
            conn.commit()
            conn.close()

        def import_host():
            conn = sqlite3.connect("airbnb.db")
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS host")
            c.execute('''
                CREATE TABLE host
                (host_id INTEGER PRIMARY KEY autoincrement, host_url TEXT, host_name TEXT, host_about TEXT, host_location TEXT) 
                ''')
            
            a = []
            for i in listing:
                host_id = i["host"]["host_id"]
                if host_id not in a:
                    a.append(host_id)
                    host_url = i["host"]["host_url"]
                    host_name = i["host"]["host_name"]
                    host_about = i["host"]["host_about"]
                    host_location = i["host"]["host_location"]
                    c.execute("INSERT INTO host (host_id, host_url, host_name, host_about, host_location) VALUES (?,?,?,?,?)",(host_id, host_url, host_name, host_about, host_location))
                
            conn.commit()
            conn.close()

    import_review()
    import_reviewer()
    import_amenities()
    import_accommodation()
    import_host_accommodation()
    import_host()
    

if __name__ == '__main__':
    start()