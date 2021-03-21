from flask import Flask, request,jsonify
import json
import sqlite3

app = Flask(__name__)
app.config['DEBUG'] = True

#Do NOT put functions/statement outside functions
@app.route('/airbnb/reviews/',methods = ['GET'])
def get_reviews():
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    if 'start' in request.args and 'end' in request.args:
        start = request.args['start']
        end = request.args['end'] + "T23:59:59Z"
        c.execute('''
    	   SELECT accommodation_id , comment , datetime , reviewer.rid , rname  
    	   FROM reviewer,review WHERE reviewer.rid = review.rid and datetime >= ? and datetime <= ? ORDER BY datetime DESC, reviewer.rid ASC
        ''',(start,end,))
    elif 'start' in request.args:
        start = request.args['start']
        c.execute('''
           SELECT accommodation_id , comment , datetime , reviewer.rid , rname  
           FROM reviewer,review WHERE reviewer.rid = review.rid and datetime >= ? ORDER BY datetime DESC, reviewer.rid ASC
        ''',(start,))
    elif 'end' in request.args:
        end = request.args['end']
        c.execute('''
           SELECT accommodation_id , comment , datetime , reviewer.rid , rname  
           FROM reviewer,review WHERE reviewer.rid = review.rid and datetime <= ? ORDER BY datetime DESC, reviewer.rid ASC
        ''',(end,))
    else:
        c.execute('''
           SELECT accommodation_id , comment , datetime , reviewer.rid , rname  
           FROM reviewer,review WHERE reviewer.rid = review.rid ORDER BY datetime DESC, reviewer.rid ASC
        ''')               

    conn.commit()
    rows = c.fetchall()
    conn.close()
    response_body = []

    for row in rows:
        response_body.append(
            {
                'Accommodation ID': row[0],
                'Comment': row[1],
                'DateTime': row[2],
                'Reviewer ID': row[3],
                'Reviewer Name': row[4]
            }
        )     
   
    count = len(response_body)
    response = {"Count": count,"Reviews":response_body}
    return jsonify(response),200, {'Content-Type': 'application/json'}

@app.route('/airbnb/reviewers/',methods = ['GET'])
def get_reviewers():
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()

    if 'sort_by_review_count' in request.args:
        sort_by_review_count = request.args['sort_by_review_count']
        if sort_by_review_count == 'ascending':
            c.execute('''
                SELECT COUNT(reviewer.rid), reviewer.rid, rname FROM reviewer,review WHERE reviewer.rid = review.rid GROUP BY reviewer.rid ORDER BY COUNT(reviewer.rid) ASC, reviewer.rid ASC
                ''')
        elif sort_by_review_count == 'descending':
            c.execute('''
                SELECT COUNT(reviewer.rid), reviewer.rid, rname FROM reviewer,review WHERE reviewer.rid = review.rid GROUP BY reviewer.rid ORDER BY COUNT(reviewer.rid) DESC, reviewer.rid ASC
                ''')
    else:
        c.execute('''
            SELECT COUNT(reviewer.rid), reviewer.rid, rname FROM reviewer,review WHERE reviewer.rid = review.rid GROUP BY reviewer.rid ORDER BY reviewer.rid ASC
            ''')

    rows = c.fetchall()
    conn.close()

    response_body=[]

    for row in rows:
        response_body.append(
            {
                'Review Count': row[0],
                'Reviewer ID': row[1],
                'Reviewer Name': row[2]
            }
            )

    count = len(response_body)
    response = {"Count": count,"Reviewers":response_body}
    return jsonify(response),200, {'Content-Type': 'application/json'}

@app.route('/airbnb/reviewers/<reviewer_id>',methods = ['GET'])    
def get_reviewer(reviewer_id):
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()        
    c.execute("SELECT rid, rname FROM reviewer WHERE rid = ?",(reviewer_id,))   
    rows = c.fetchall()
    if len(rows) == 0:
        response = {"Reasons":[{"Message": "Reviewer not found"}]}
        conn.close()
        return jsonify(response),404,{'Content-Type': 'application/json'}
    else:
        c.execute("SELECT accommodation_id,comment,datetime FROM review WHERE review.rid = ? ORDER BY datetime DESC",(reviewer_id,))
        reviews = c.fetchall()  
        r = []
        for review in reviews:
            r.append(
                {
                    "Accommodation ID": review[0],
                    "Comment": review[1],
                    "DateTime": review[2]
                }
                )
        response = {"Reviewer ID":rows[0][0],"Reviewer Name":rows[0][1],"Reviews":r}
        conn.close()   
        return jsonify(response),200,{'Content-Type': 'application/json'}

@app.route('/airbnb/hosts/',methods=['GET'])
def hosts():
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    if 'sort_by_accommodation_count' in request.args:
        sort_by_accommodation_count = request.args['sort_by_accommodation_count']
        if sort_by_accommodation_count == 'ascending':
            c.execute('''SELECT COUNT(host_accommodation.host_id),host_about,host.host_id,host_location,host_name,host_url
                FROM host_accommodation,host WHERE host.host_id = host_accommodation.host_id GROUP BY host.host_id ORDER BY COUNT(host_accommodation.host_id) ASC,host.host_id ASC''')
        else:
            c.execute('''SELECT COUNT(host_accommodation.host_id),host_about,host.host_id,host_location,host_name,host_url
                FROM host_accommodation,host WHERE host.host_id = host_accommodation.host_id GROUP BY host.host_id ORDER BY COUNT(host_accommodation.host_id) DESC,host.host_id ASC''')
    else:
        c.execute('''SELECT COUNT(host_accommodation.host_id),host_about,host.host_id,host_location,host_name,host_url
            FROM host_accommodation,host WHERE host.host_id = host_accommodation.host_id GROUP BY host.host_id ORDER BY host.host_id ASC''')     
    hosts = c.fetchall()
    h = []
    for host in hosts:
        h.append({
            "Accommodation Count": host[0],
            "Host About": host[1],
            "Host ID": host[2],
            "Host Location": host[3],
            "Host Name": host[4],
            "Host URL": host[5]
            })
    count = len(h)
    response = {"Count": count,"Hosts":h}
    return jsonify(response),200,{'Content-Type': 'application/json'}

@app.route('/airbnb/hosts/<host_id>',methods=['GET'])
def host(host_id):
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    c.execute("SELECT accommodation_id FROM host_accommodation WHERE host_id =?",(host_id,))
    accommodation_ids = c.fetchall()
    if len(accommodation_ids) == 0:
        response = {"Reasons":[{"Message": "Host not found"}]}
        conn.close()   
        return jsonify(response),404,{'Content-Type': 'application/json'}
    a = []
    for accommodation_id in accommodation_ids:
        c.execute("SELECT id, name FROM accommodation WHERE id = ?",(accommodation_id[0],))
        accommodations = c.fetchall()
        for accommodation in accommodations:
            a.append({
                "Accommodation ID": accommodation[0],
                "Accommodation Name": accommodation[1]
                })
    c.execute('''SELECT COUNT(host_accommodation.host_id),host_about,host.host_id,host_location,host_name,host_url
            FROM host_accommodation,host WHERE host.host_id = ?  and host.host_id = host_accommodation.host_id GROUP BY host.host_id ORDER BY host_accommodation.host_id ASC''',(host_id,))
    host = c.fetchall()
    h = {   "Accommodation Count": host[0][0],
            "Host About": host[0][1],
            "Host ID": host[0][2],
            "Host Location": host[0][3],
            "Host Name": host[0][4],
            "Host URL": host[0][5] 
        }

    response = {"Accommodation":a, 
            "Accommodation Count": host[0][0],
            "Host About": host[0][1],
            "Host ID": host[0][2],
            "Host Location": host[0][3],
            "Host Name": host[0][4],
            "Host URL": host[0][5] 
            }
    conn.close()
    return jsonify(response), 200, {'Content-Type': 'application/json'} 

@app.route('/airbnb/accommodations/',methods=['GET'])
def get_accommodation():
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    if 'min_review_score_value' in request.args and 'amenities' in request.args:
        amenities = request.args['amenities']
        min_review_score_value = request.args['min_review_score_value']
        c.execute('''SELECT review.accommodation_id,COUNT(review.accommodation_id),review_score_value FROM accommodation,review,amenities
        WHERE accommodation.id = review.accommodation_id and amenities.accommodation_id = review.accommodation_id  and accommodation.id = amenities.accommodation_id and review_score_value >= ? and type = ?
        GROUP BY review.accommodation_id ORDER BY accommodation.id ASC''',(min_review_score_value,amenities,))
    elif 'min_review_score_value' in request.args:
        min_review_score_value = request.args['min_review_score_value']
        c.execute('''SELECT review.accommodation_id,COUNT(review.accommodation_id),review_score_value FROM accommodation,review
        WHERE accommodation.id = review.accommodation_id and review_score_value >= ?
        GROUP BY review.accommodation_id ORDER BY accommodation.id ASC''',(min_review_score_value,))        
    elif 'amenities' in request.args:
        amenities = request.args['amenities']
        c.execute('''SELECT review.accommodation_id,COUNT(review.accommodation_id),review_score_value FROM accommodation,review,amenities
        WHERE accommodation.id = review.accommodation_id and amenities.accommodation_id = review.accommodation_id and accommodation.id = amenities.accommodation_id and type = ?
        GROUP BY review.accommodation_id ORDER BY accommodation.id ASC''',(amenities,))
    else:
        c.execute('''SELECT accommodation_id,COUNT(review.accommodation_id),review_score_value FROM accommodation,review
        WHERE accommodation.id = review.accommodation_id GROUP BY review.accommodation_id ORDER BY accommodation.id ASC''')
    aids = c.fetchall()
    Accommodations = []
    for aid in aids:
        c.execute("SELECT name,summary,url FROM accommodation WHERE id =?",(aid[0],))
        accommodation = c.fetchall()
        accommodation_item ={
        'Name': accommodation[0][0],
        'Summary': accommodation[0][1],
        'URL': accommodation[0][2]
        }
        c.execute("SELECT type FROM amenities WHERE accommodation_id =?",(aid[0],))
        Amenities = c.fetchall()
        a = []
        for Amenitie in Amenities:
            a.append(Amenitie[0])
        c.execute('''SELECT host_about,host.host_id,host_location,host_name FROM host,host_accommodation 
            WHERE host.host_id = host_accommodation.host_id and accommodation_id = ?''',(aid[0],))
        h = c.fetchall()
        host = {
        'About': h[0][0],
        'ID': h[0][1],
        'Location': h[0][2],
        'Name': h[0][3]
        }
        A = {
        'Accommodation':accommodation_item,
        'Amenities': a,
        'Host': host,
        'ID': aid[0],
        'Review Count':aid[1],
        'Review Score Value': aid[2]
        }
        Accommodations.append(A)
    response = {'Accommodations':Accommodations,'Count':len(Accommodations)}
    conn.close()
    return jsonify(response), 200, {'Content-Type': 'application/json'}

@app.route('/airbnb/accommodations/<accommodation_id>',methods=['GET'])
def get_accommodation_by_id(accommodation_id):
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()    
    c.execute("SELECT id,name,review_score_value,summary,url FROM accommodation WHERE id = ?",(accommodation_id,))
    accommodation = c.fetchall()
    if len(accommodation) == 0:
        response = {"Reasons":[{"Message": "Accommodation not found"}]}
        conn.close()   
        return jsonify(response),404,{'Content-Type': 'application/json'}
    c.execute("SELECT type FROM amenities WHERE accommodation_id =? ORDER BY type ASC",(accommodation_id,))
    amenities = c.fetchall()
    a = []
    for amenitie in amenities:
        a.append(amenitie[0])
    c.execute('''SELECT comment,datetime,rname,review.rid FROM review,reviewer
        WHERE review.rid = reviewer.rid and accommodation_id=? ORDER BY review.rid DESC''',(accommodation_id,))
    reviews = c.fetchall()
    r = []
    for review in reviews:
        r.append({
        'Comment' : review[0],
        'DateTime': review[1],
        'Reviewer Name': review[2],
        'Reviewer ID': review[3]
        })
    response = {
    'Accommodation ID': accommodation[0][0],
    'Accommodation Name': accommodation[0][1],
    'Amenities': a,
    'Review Score Value': accommodation[0][2],
    'Reviews': r,
    'Summary': accommodation[0][3],
    'URL': accommodation[0][4]
    }
    conn.close()
    return jsonify(response), 200, {'Content-Type': 'application/json'}     
    



# Show your student ID
@app.route('/mystudentID/', methods=['GET'])
def my_student_id():    
    response={"studentID": "20028987D"}
    return jsonify(response), 200, {'Content-Type': 'application/json'} 

if __name__ == '__main__':
   app.run()

