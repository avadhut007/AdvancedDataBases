#Name: Avadhut Vijay Talbar
#UTA ID: 1001955622
#CSE6331
#Assignment3

from flask import Flask, flash, render_template, request, redirect

import pyodbc
import csv

from datetime import datetime
import haversine as hs
import requests

from settings import server, database, username, password, driver, mapQuest_key, mapQuest_url, myRedisHostname, myRedisPassword

import time
import redis
import pickle
import random

connstr = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password



try:
    redisClient = redis.StrictRedis(host=myRedisHostname, port=6380,
                        password=myRedisPassword, ssl=True)

    resultred = redisClient.ping()
    print("Ping returned : " + str(resultred))
except Exception as e:
        print(e,"Error connecting to Redis")


app = Flask(__name__)


#SQL

def upload_csv(filename): # path not required since we will select manually
    try:
        conn = pyodbc.connect(connstr)
        #print("conn",conn)
        cursor = conn.cursor()
        #print("cursor",cursor)
        path = './'
        count = 0
        table = 'av_people'
        with open (filename, 'r') as file:
            
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                #row.pop()
                row = ['NULL' if val == '' or val == '-1' else val for val in row]
                row = [x.replace("'", "''") for x in row]
                out = "'" + "', '".join(str(item) for item in row) + "'"
                out = out.replace("'NULL'", 'NULL')
                query = "INSERT INTO " + table + " VALUES (" + out + ")"
                #print("query",query)
                cursor.execute(query)
                count+=1
            cursor.commit()
    
    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()
    print("Added " + str(count) + " rows into table " + table)


def run_search_query(query):
    list_result=[]
    try:
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(query)
        
        list_result = cursor.fetchall()
        #print(list_result,"type",type(list_result[0]))

    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()
    return list_result

def run_update_query(query):
    list_result=[]
    try:
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.commit()

        list_result = cursor.fetchall() # check here if error
        #print(list_result,"type")

    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()
    return list_result

###### Redis Functions ######

# def redis_search_query(query):
#     try:
#         conn = pyodbc.connect(connstr)
#         cursor = conn.cursor()
#         result = cursor.execute(query)
#         list_result = []
#         for row in result:
#             record = ({})
#             for i, columns in enumerate(cursor.description):
#                 record.update({columns[0]: row[i]})
#             list_result.append(record)
#         print("redis searc",type(list_result[0]))
#     except Exception as e:
#         print(e,"Error connecting DB")
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()
#     return list_result


###### Routes ######

@app.route("/", methods=['GET','POST'])
@app.route("/home", methods=['GET','POST'])
def home_function():
    if request.method == 'POST':
        try:
            if 'clear_cache_button' in request.form:
                redisClient.flushall()
                return render_template('home.html')


            if 'from_id_q5' in request.form:
                #print(request.form["search_name"])
                from_id = int(request.form["from_id_q5"])
                to_id = int(request.form["to_id_q5"])
                print("here")
                search_query = "SELECT * FROM ni WHERE 1 = 1 "
                search_query += " AND id between " + str(from_id) + " AND " + str(to_id) + " ORDER BY id"
                print(search_query)
                list_result_q5 = run_search_query(search_query)
                print(list_result_q5)
                smallest = list_result_q5[0]
                print(search_query)
                largest = list_result_q5[len(list_result_q5)-1]
                statement_q5 = f"The smallest user and id is  {smallest} and largest user and id {largest}."
                
                t_headings_q5 = ["name","id"]

                return render_template('home.html',t_headings_q5=t_headings_q5,list_result_q5=list_result_q5,statement_q5=statement_q5)               

            if 'from_id_q6' in request.form:
                #print(request.form["search_name"])
                n_times = int(request.form["n_times_q6"])
                from_id = int(request.form["from_id_q6"])
                to_id = int(request.form["to_id_q6"])
                print("here")
                redis_key = "que6"
                init_time = time.time()
                for i in range(0,n_times):
                    search_query = "SELECT ni.id,ni.name,di.pwd,di.code FROM ni join di on ni.id=di.id WHERE 1 = 1 "
                    search_query += " AND ni.id between " + str(from_id) + " AND " + str(to_id) + " ORDER BY id"
                    print(search_query)
                    list_result_q6 = run_search_query(search_query)
                    print(list_result_q6)
                    smallest = list_result_q6[0]
                    print(search_query)
                    largest = list_result_q6[len(list_result_q6)-1]
                
                    

                    redis_key_exists = redisClient.exists(redis_key)

                    if not redis_key_exists:
                        list_result_q6 = run_search_query(search_query)
                        redisClient.set(redis_key,pickle.dumps(list_result_q6))
                        #print("inside None")
                    else:
                        list_result_q6 = redisClient.get(redis_key)
                        #print("pickle",list_result33)
                    final_time = time.time()
                    exec_time = final_time - init_time
                    list_result_q6 = pickle.loads(list_result_q6)

                final_time = time.time()
                exec_time = final_time - init_time
                statement_q6 = f"The Execution time is  {exec_time}  seconds for running this query  {n_times}  times."
                
                t_headings_q6 = ["id","name","pwd","code"]

                return render_template('home.html',t_headings_q6=t_headings_q6,list_result_q6=list_result_q6,statement_q6=statement_q6)               

            if 'top_num_q62' in request.form:
                #print(request.form["search_name"])
                n_times = int(request.form["n_times_q62"])
                top_num = int(request.form["top_num_q62"])
                code_q62 = int(request.form["code_q62"])
                print("here")
                redis_key = "que62"
                init_time = time.time()
                for i in range(0,n_times):
                    search_query = "SELECT TOP "+str(top_num) +" ni.id,ni.name,di.pwd,di.code FROM ni join di on ni.id=di.id WHERE 1 = 1 "
                    search_query += " AND di.code = " + str(code_q62) +""
                    print(search_query)
                
                    

                    redis_key_exists = redisClient.exists(redis_key)

                    if not redis_key_exists:
                        list_result_q62 = run_search_query(search_query)
                        redisClient.set(redis_key,pickle.dumps(list_result_q62))
                        #print("inside None")
                    else:
                        list_result_q62 = redisClient.get(redis_key)
                        #print("pickle",list_result33)
                    final_time = time.time()
                    exec_time = final_time - init_time
                    list_result_q62 = pickle.loads(list_result_q62)
                print(list_result_q62)
                final_time = time.time()
                exec_time = final_time - init_time
                print(search_query)
               
                statement_q62 = f"The Execution time is  {exec_time}  seconds for running this query {n_times}  times."
                
                t_headings_q62 = ["id","name","pwd","code"]

                return render_template('home.html',t_headings_q62=t_headings_q62,list_result_q62=list_result_q62,statement_q62=statement_q62)               











            if 'n_times_31' in request.form:
                #print(request.form["search_name"])
                n_times = request.form["n_times_31"]
                if int(n_times) >=0 and int(n_times) <= 1000:
                    n_times = int(n_times)
                    init_time = time.time()
                    for i in range(0,n_times):
                        search_query = "SELECT * FROM av_asg3 WHERE 1 = 1 "
                        list_result31 = run_search_query(search_query)
                    final_time = time.time()
                    exec_time = final_time - init_time
                    statement_31 = f"The Execution time is  {exec_time}  seconds for running this query {n_times} times."
                    count_rows31 = f"The total count of earthquakes occured is {(len(list_result31))}"

                return render_template('home.html',count_rows31=count_rows31,statement_31=statement_31)
            
            if 'n_times_32' in request.form:
                #print(request.form["search_name"])
                n_times = request.form["n_times_32"]
                from_date = request.form["from_date_32"]
                to_date = request.form["to_date_32"]
                if int(n_times) >=0 and int(n_times) <= 1000:
                    n_times = int(n_times)
                    init_time = time.time()
                    for i in range(0,n_times):
                        search_query = "SELECT * FROM av_asg3 WHERE 1 = 1 "
                        if len(from_date) !=0  and len(to_date) !=0 :
                            search_query+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"
                        print("search",search_query)
                        list_result32 = run_search_query(search_query)
                    final_time = time.time()
                    exec_time = final_time - init_time
                    statement_32 = f"The Execution time is  {exec_time}  seconds for running this query {n_times} times."
                    count_rows32 = f"The total count of earthquakes occured is {(len(list_result32))}  from {from_date} to {to_date}."

                return render_template('home.html',count_rows32=count_rows32,statement_32=statement_32)



            if 'n_times_35' in request.form:
                #print(request.form["search_name"])
                n_times = '4'
                from_lat= '2.0'
                to_lat = '6.0'
                from_lat=float(from_lat)
                to_lat=float(to_lat)
                if int(n_times) >=0 and int(n_times) <= 1000:
                    n_times = int(n_times)
                    final_result_35 = []
                    for i in range(0,n_times):
                        pair_of_ran_lat = [round(random.uniform(from_lat,to_lat),2),round(random.uniform(from_lat,to_lat),2)]
                        ran_from_lat = min(pair_of_ran_lat)
                        ran_to_lat = max(pair_of_ran_lat)
                        init_time = time.time()
                        search_query = "SELECT * FROM av_asg3 WHERE 1 = 1 "
                        
                        search_query+=" AND latitude between '" + str(ran_from_lat) + "' AND '" + str(ran_to_lat) + "'"
                        
                        print("search",search_query)
                        
                        list_result35 = run_search_query(search_query)
                        final_time = time.time()
                        exec_time = final_time - init_time
                        final_result_35.append([ran_from_lat,ran_to_lat,len(list_result35),exec_time])

                        
                    #statement_32 = f"The Execution time is  {exec_time}  seconds for running this query {n_times} times."
                    #count_rows32 = f"The total count of earthquakes occured is {(len(list_result1))}  from {from_date} to {to_date}."
                   
                return render_template('home.html',final_result_35=final_result_35)







########## Redis Cache
            
            if 'n_times_33' in request.form:
                #print(request.form["search_name"])
                n_times = request.form["n_times_33"]
                if int(n_times) >=0 and int(n_times) <= 1000:
                    n_times = int(n_times)
                    init_time = time.time()
                    redis_key = "restricted_query"
                    search_query = "SELECT * FROM av_asg3 WHERE 1 = 1 "

                    for i in range(0,n_times):
                        redis_key_exists = redisClient.exists(redis_key)

                        if not redis_key_exists:
                            list_result33 = run_search_query(search_query)
                            redisClient.set(redis_key,pickle.dumps(list_result33))
                            #print("inside None")
                        else:
                            list_result33 = redisClient.get(redis_key)
                            #print("pickle",list_result33)
                    final_time = time.time()
                    exec_time = final_time - init_time
                    list_result33 = pickle.loads(list_result33)

                    statement_33 = f"The Execution time is  {exec_time}  seconds for running this query {n_times} times."
                    count_rows33 = f"The total count of earthquakes occured is {(len(list_result33))}"

                return render_template('home.html',count_rows33=count_rows33,statement_33=statement_33,scroll_33="scroll_33")
            

            if 'n_times_34' in request.form:
                #print(request.form["search_name"])
                n_times = request.form["n_times_34"]
                from_date = request.form["from_date_34"]
                to_date = request.form["to_date_34"]
                if int(n_times) >=0 and int(n_times) <= 1000:
                    n_times = int(n_times)
                    init_time = time.time()
                    redis_key = "unrestricted_query"
                    search_query = "SELECT * FROM av_asg3 WHERE 1 = 1 "
                    if len(from_date) !=0  and len(to_date) !=0 :
                            search_query+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"

                    for i in range(0,n_times):
                        redis_key_exists = redisClient.exists(redis_key)

                        if not redis_key_exists:
                            list_result34 = run_search_query(search_query)
                            redisClient.set(redis_key,pickle.dumps(list_result34))
                            #print("inside None")
                        else:
                            list_result34 = redisClient.get(redis_key)
                            #print("pickle",list_result33)
                    final_time = time.time()
                    exec_time = final_time - init_time
                    list_result34 = pickle.loads(list_result34)

                    statement_34 = f"The Execution time is  {exec_time}  seconds for running this query {n_times} times."
                    count_rows34 = f"The total count of earthquakes occured is {(len(list_result34))}  from {from_date} to {to_date}."

                return render_template('home.html',count_rows34=count_rows34,statement_34=statement_34,scroll_34="scroll_34")




















































######## Retrieve Data in /home ######

            if 'search_mag' in request.form:
                #print(request.form["search_name"])
                magnitude = request.form["search_mag"]
                from_date = request.form["search_from_date"]
                to_date = request.form["search_to_date"]
                #print(type(from_date),from_date)
                search_query = "SELECT * FROM av_asg3 WHERE 1 = 1 "
                if len(magnitude)!=0:
                    magnitude = float(magnitude)
                    search_query+= "AND mag > " + str(magnitude)
                if len(from_date) !=0  and len(to_date) !=0 :
                    search_query+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"
                #print(search_query)
                list_result1 = run_search_query(search_query)
                count_rows = f"The count of earthquakes occured with magnitude greather than {magnitude} is {(len(list_result1))} from {from_date} to {to_date}"
                
                t_headings1 = ["time", "latitude","longitude", "depth","mag","magType","nst","gap","dmin","rms","net","id","updated","place","type","horizontalError","depthError","magError","magNst","status","locationSource","magSource"]

                return render_template('home.html',count_rows=count_rows,t_headings1=t_headings1,list_result1=list_result1,scroll1="scroll1")
            
            if 'dist_dist' in request.form:
                location = request.form["dist_loc"]
                latitude = request.form["dist_lat"]
                longitude = request.form["dist_long"]
                from_date = request.form["dist_from_date"]
                to_date = request.form["dist_to_date"]
                if len(location)!=0:
                    main_url = mapQuest_url+mapQuest_key+'&location='+location
                    print(main_url)

                    location_data = requests.get(main_url).json()['results'][0]['locations'][0]['latLng']
                    print("location_data=",location_data)
                    latitude = location_data['lat']
                    longitude = location_data['lng']
                    #print("lat and lang=",latitude,longitude)

                distance = request.form["dist_dist"]
                if len(distance)!=0:
                    distance = float(distance)

                search_query = "SELECT * FROM av_asg3 WHERE 1 = 1 "
                if len(from_date) !=0  and len(to_date) !=0 :
                    search_query+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"
                search_query+=" ORDER By mag DESC"
                list_result2 = run_search_query(search_query)
                location1 = (float(latitude),float(longitude))
                    
                list_result2_updated = []
                for item in list_result2:
                    location2 = (float(item[1]),float(item[2]))
                    actual_diff = hs.haversine(location1,location2)
                    if distance >= actual_diff:
                        list_result2_updated.append(item)
                
                count_rows2 = f"The count of earthquakes from {location} lat= {latitude} and lng= {longitude} within {distance} km is {len(list_result2_updated)} from {from_date} to {to_date}"
                
                t_headings2 = ["time", "latitude","longitude", "depth","mag","magType","nst","gap","dmin","rms","net","id","updated","place","type","horizontalError","depthError","magError","magNst","status","locationSource","magSource"]

                return render_template('home.html',count_rows2=count_rows2,t_headings2=t_headings2,list_result2_updated=list_result2_updated,scroll2="scroll2")
            


        except Exception as e:
            print(e,"Error has occured")
            

    return render_template('home.html')
















######### UPDATE PAGE #########
@app.route("/update", methods=['GET','POST'])
def update_function():
    if request.method == 'POST':
        if 'update_name' in request.form :
            pass

    return render_template("update.html")

######### UPLOAD PAGE #########

app.config['ALLOWED_CSV_TYPE'] = ['CSV']

def allowed_csv(filename):
    if not '.' in filename:
        return False
    ext = filename.rsplit('.',1)[1]

    if ext.upper() in app.config['ALLOWED_CSV_TYPE']:
        return True
    else:
        return False

@app.route("/upload", methods=['GET','POST'] )
def upload_function():
    if request.method == 'POST':
        if 'csvfile' in request.files:

            csvfile = request.files["csvfile"]

            if not allowed_csv(csvfile.filename):
                print('file extension not allowed')
                #flash('file extension not allowed')
                return render_template("upload.html")
            #print(connstr,"connstr")
            upload_csv(csvfile.filename)


            print("csv uploaded")

    return render_template("upload.html")



if __name__ == '__main__':
    app.run(debug=True)

