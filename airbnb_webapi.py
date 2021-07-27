from flask import Flask, request,jsonify
import json
import sqlite3

app = Flask(__name__)
app.config['DEBUG'] = True

#Do NOT put functions/statement outside functions

# Show your student ID
@app.route('/mystudentID/', methods=['GET'])
def my_student_id():    
    response={"studentID": "xxxxxxxxx"}
    return jsonify(response), 200, {'Content-Type': 'application/json'}

#b
@app.route('/airbnb/reviews/', methods=['GET'])
def airbnb_reviews():
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    if 'start' in request.args.keys() and 'end' in request.args.keys():
        a = request.args['start']
        b = request.args['end']
        data = c.execute(" select accommodation_id, comment, datetime, reviewer.rid, rname \
              from review, reviewer \
              where review.rid = reviewer.rid and datetime BETWEEN (?) and (?) \
              order by review.rid ASC", (a,b)).fetchall()
    
    elif 'start' in request.args.keys() and 'end' not in request.args.keys():
        a = request.args['start']

        data = c.execute(" select accommodation_id, comment, datetime, reviewer.rid, rname \
              from review, reviewer \
              where review.rid = reviewer.rid and datetime >= (?) \
              order by review.rid ASC", (a)).fetchall()
    
    elif 'start' not in request.args.keys() and 'end' in request.args.keys():
        b = request.args['end']

        data = c.execute(" select accommodation_id, comment, datetime, reviewer.rid, rname \
              from review, reviewer \
              where review.rid = reviewer.rid and datetime <= (?) \
              order by review.rid ASC", (b)).fetchall()

    else:
        data = c.execute("Select accommodation_id, comment, datetime, reviewer.rid, rname \
                     from review, reviewer \
                     where review.rid = reviewer.rid \
                     ORDER BY datetime DESC, review.rid ASC").fetchall()
    reviews = []
    for row in data:
        reviews.append({
            "Accommodation ID": row[0],
            "Comment": row[1],
            "DateTime": row[2],
            "Reviewer ID": row[3],
            "Reviewer Name": row[4]})

    table = {"Count": len(reviews), "Reviews": reviews}

    result = json.dumps(table)
    return result, 200, {'Content-Type': 'application/json'}

    conn.close()

#c1
@app.route('/airbnb/reviewers/', methods=['GET'])
def airbnb_reviewers():
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    if 'sort_by_review_count' in request.args.keys():
        if "ascending" ==  request.args['sort_by_review_count']:
            data = c.execute(" select reviewer.rid, rname, count(id) \
                           from review, reviewer \
                           where review.rid = reviewer.rid \
                           group by reviewer.rid \
                           order by count(id) asc, reviewer.rid asc ").fetchall()
            
        elif "descending" ==  request.args['sort_by_review_count']:
            data = c.execute(" select reviewer.rid, rname, count(id) \
                                from review, reviewer \
                                where review.rid = reviewer.rid \
                                group by reviewer.rid \
                                order by count(id) desc, reviewer.rid asc ").fetchall()
           
    else:
        data = c.execute(" select reviewer.rid, rname, count(id) \
                       from review, reviewer \
                       where review.rid = reviewer.rid \
                       group by reviewer.rid \
                       order by reviewer.rid asc ").fetchall()

    reviewers = []
    for row in data:
        reviewers.append({
            "Review Count": row[2],
            "Reviewer ID": row[0],
            "Reviewer Name": row[1]})

    result = {"Count": len(reviewers), "Reviewers": reviewers}

    table = json.dumps(result)
    return table, 200, {'Content-Type': 'application/json'}
        
    conn.close()
#c2
@app.route('/airbnb/reviewers/<rid>', methods=['GET'])
def get_reviewerID(rid):
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    data1 = c.execute("select rid from reviewer").fetchall()
    table1 = []
    for row in data1:
        table1.append(row[0])

    data2 = c.execute("select rname from reviewer where rid = ?", [rid]).fetchall()
    table2 = []
    for row in data2:
        table2.append(row[0])
    
    data3 = c.execute("select accommodation_id, comment, datetime \
                       from review \
                       where rid = ? \
                       order by datetime desc",[rid]).fetchall()
    table3=[]
    for row in data3:
        accommodation_id = row[0]
        Comment = row[1]
        Datetime = row[2] 

        table3.append({
        "Accommodation ID": accommodation_id,
        "Comment": Comment,
        "DateTime": Datetime})
    
    if int(rid) in table1:
        return jsonify({"Reviewer ID": int(rid),"Reviewer Name": table2[0], "Reviews": table3}), 200, {'Content-Type': 'application/json'}
    else:
        return jsonify({"Reasons": [{'Message': 'Reviewer not found'}]}), 404 
    conn.close()  
#d
@app.route('/airbnb/hosts/', methods=['GET'])
def airbnb_hosts():
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    if 'sort_by_accommodation_count' in request.args.keys():
        if "ascending" ==  request.args['sort_by_accommodation_count']:
            data = c.execute(" select count(host_accommodation.accommodation_id), host_about, host.host_id, host_location, host_name, host_url\
                               from host, host_accommodation\
                               where host.host_id = host_accommodation.host_id\
                               group by host.host_id\
                               order by count(accommodation_id) asc, host.host_id asc ").fetchall()
            table = []
            for row in data:
                table.append({
                    "Accommodation Count": row[0],
                    "Host About": row[1],
                    "Host ID": row[2],
                    "Host Location": row[3],
                    "Host Name": row[4],
                    "Host URL": row[5]
                })
                    
            result = {"Count": len(table),"Hosts": table}
            tresults = json.dumps(result)

            return tresults, 200, {'Content-Type': 'application/json'}

        elif "descending" ==  request.args['sort_by_accommodation_count']:
            data1 = c.execute(" select count(host_accommodation.accommodation_id), host_about, host.host_id, host_location, host_name, host_url\
                               from host, host_accommodation\
                               where host.host_id = host_accommodation.host_id\
                               group by host.host_id\
                               order by count(accommodation_id) desc, host.host_id asc ").fetchall()
            table1 = []
            for row in data1:
                table1.append({
                    "Accommodation Count": row[0],
                    "Host About": row[1],
                    "Host ID": row[2],
                    "Host Location": row[3],
                    "Host Name": row[4],
                    "Host URL": row[5]
                })       
            result = {"Count": len(table1),"Hosts": table1}
            tresults = json.dumps(result)

            return tresults, 200, {'Content-Type': 'application/json'}
    else:
        data2 = c.execute(" select count(host_accommodation.accommodation_id), host_about, host.host_id, host_location, host_name, host_url\
                            from host, host_accommodation\
                            where host.host_id = host_accommodation.host_id\
                            group by host.host_id\
                            order by host.host_id asc ").fetchall()
        table2 = []
        for row in data2:
            table2.append({
                "Accommodation Count": row[0],
                "Host About": row[1],
                "Host ID": row[2],
                "Host Location": row[3],
                "Host Name": row[4],
                "Host URL": row[5]
            })
                    
        result = {"Count": len(table2),"Hosts": table2}
        tresults = json.dumps(result)

        return tresults, 200, {'Content-Type': 'application/json'}
    conn.close()
 
#d2
@app.route('/airbnb/hosts/<host_id>', methods=['GET'])
def get_hostID(host_id):
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()
    data1 = c.execute("select accommodation_id, name \
                       from host_accommodation, accommodation \
                       where host_accommodation.accommodation_id = accommodation.id and host_id = ?\
                       order by accommodation_id asc", [host_id]).fetchall()
    table1 = []
    for row in data1:
        accommodation_id = row[0]
        accommodationname = row[1]
        table1.append({
            "Accommodation ID" : accommodation_id,
            "Accommodation Name" : accommodationname})

    data2 = c.execute("select count(host.host_id) from host where host_id = ?", [host_id]).fetchall()
    table2 = []
    for row in data2:
        table2.append(row[0])
    
    data3 = c.execute("select host_about from host where host_id = ?", [host_id]).fetchall()
    table3 = []
    for row in data3:
        table3.append(row[0])
    
    data4 = c.execute("select host_id from host where host_id").fetchall()
    table4 = []
    for row in data4:
        table4.append(row[0])

    data5 = c.execute("select host_location from host where host_id = ?", [host_id]).fetchall()
    table5 = []
    for row in data5:
        table5.append(row[0])

    data6 = c.execute("select host_name from host where host_id = ?", [host_id]).fetchall()
    table6 = []
    for row in data6:
        table6.append(row[0])

    data7 = c.execute("select host_url from host where host_id = ?", [host_id]).fetchall()
    table7 = []
    for row in data7:
        table7.append(row[0])

    table = {"Accommodation": table1,
             "Accommodation Count": table2[0], 
             "Host About": table3[0], 
             "Host ID": int(host_id), 
             "Host Location": table5[0], 
             "Host Name": table6[0], 
             "Host URL": table7[0]}
    
    if int(host_id) in table4:
        return json.dumps(table), 200, {'Content-Type': 'application/json'}
    else:
        return json.dumps({"Reasons": [{'Message': 'Host not found'}]}), 404 
    conn.close()

#e
@app.route('/airbnb/accommodations/', methods=['GET'])
def get_all_accommodation():
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()

    if 'min_review_score_value' in request.args.keys() and 'amenities' not in request.args.keys():
        a = request.args['min_review_score_value']
        data = c.execute("select name, summary, url, (SELECT group_concat(type) FROM amenities where accommodation.id = amenities.accommodation_id group by amenities.accommodation_id) as type\
                         , host_about, host.host_id, host_location, host_name, review.accommodation_id, count(review.id), review_score_value\
                         from accommodation\
                         join review on accommodation.id = review.accommodation_id\
                         join host_accommodation on host_accommodation.accommodation_id = accommodation.id\
                         join host on host_accommodation.host_id = host.host_id\
                         where review_score_value >= (?)\
                         group by review.accommodation_id\
                         order by count(review.accommodation_id) asc; ", [a]).fetchall()

    elif 'amenities' in request.args.keys() and 'min_review_score_value' not in request.args.keys():
        b = request.args['amenities']
        data = c.execute("select name, summary, url, (SELECT group_concat(type) FROM amenities where accommodation.id = amenities.accommodation_id group by amenities.accommodation_id) as type\
                         , host_about, host.host_id, host_location, host_name, review.accommodation_id, count(review.id), review_score_value\
                         from accommodation\
                         join review on accommodation.id = review.accommodation_id\
                         join host_accommodation on host_accommodation.accommodation_id = accommodation.id\
                         join host on host_accommodation.host_id = host.host_id\
                         where type like (?) \
                         group by review.accommodation_id\
                         order by count(review.accommodation_id) asc; ", ('%{}%'.format(b),)).fetchall()

    elif 'amenities' in request.args.keys() and 'min_review_score_value' in request.args.keys():
        a = request.args['min_review_score_value']
        b = request.args['amenities']
        data = c.execute("select name, summary, url, (SELECT group_concat(type) FROM amenities where accommodation.id = amenities.accommodation_id group by amenities.accommodation_id) as type\
                         , host_about, host.host_id, host_location, host_name, review.accommodation_id, count(review.id), review_score_value\
                         from accommodation\
                         join review on accommodation.id = review.accommodation_id\
                         join host_accommodation on host_accommodation.accommodation_id = accommodation.id\
                         join host on host_accommodation.host_id = host.host_id\
                         where type like (?) and review_score_value >= (?)\
                         group by review.accommodation_id\
                         order by count(review.accommodation_id) asc; ",('%'+b+'%', a)).fetchall()

    else:
        data = c.execute("select name, summary, url, (SELECT group_concat(type) FROM amenities where accommodation.id = amenities.accommodation_id group by amenities.accommodation_id) as type\
                         , host_about, host.host_id, host_location, host_name, review.accommodation_id, count(review.id), review_score_value\
                         from accommodation\
                         join review on accommodation.id = review.accommodation_id\
                         join host_accommodation on host_accommodation.accommodation_id = accommodation.id\
                         join host on host_accommodation.host_id = host.host_id\
                         group by review.accommodation_id\
                         order by count(review.accommodation_id) asc; ").fetchall()

    result = []
    for row in data:
        result.append({
            "Accommodation": {"Name": row[0], "Summary": row[1], "URL": row[2]},
            "Amenities": row[3].split(','),
            "Host": {"About": row[4], "ID": row[5], "Location": row[6], "Name": row[7]},
            "ID": row[8],
            "Review Count": row[9],
            "Review Score Value": row[10]})

    return json.dumps({"Accommodations": result, "Count": len(result)}), 200, {'Content-Type': 'application/json'}
    conn.close()

@app.route('/airbnb/accommodations/<Accommodation_ID>', methods=['GET'])
def get_accommodation_id(Accommodation_ID):
    conn = sqlite3.connect("airbnb.db")
    c = conn.cursor()

    data1 = c.execute("select id from accommodation").fetchall()
    aid = []
    for row in data1:
        aid.append(row[0])

    data2 = c.execute("select name,(SELECT group_concat(type) FROM amenities where accommodation.id = amenities.accommodation_id group by amenities.accommodation_id order by type asc ) as type,\
                       review_score_value, comment, datetime, review.rid, reviewer.rname, summary, url  \
                       from accommodation\
                       join review on accommodation.id = review.accommodation_id \
                       join reviewer on review.rid = reviewer.rid\
                       where accommodation.id = (?)\
                       order by datetime desc", [Accommodation_ID]).fetchall()
    
    Aname = []
    amenities = []
    score = []
    reviews = []
    summary = []
    url = []
    
    for row in data2:
        Aname.append(row[0])
        amenities.append(row[1].split(','))
        score.append(row[2])
        reviews.append({"Comment": row[3], "DateTime": row[4], "Reviewer ID": row[5], "Reviewer Name": row[6]})
        summary.append(row[7])
        url.append(row[8])


        results = {"Accommodation ID": int(Accommodation_ID),
                   "Accommodation Name": Aname[0],
                   "Amenities": amenities[0],
                   "Review Score Value": score[0],
                   "Reviews": reviews,
                   "Summary": summary[0],
                   "URL": url[0]}

    if int(Accommodation_ID) in aid:
        return jsonify(results), 200, {'Content-Type': 'application/json'}
    else:
        return jsonify({"Reasons": [{'Message': 'Accommodation not found'}]}), 404 
    conn.close() 

if __name__ == '__main__':
   app.run(host='0.0.0.0', port=5000, debug=True)

