#Name: Avadhut Vijay Talbar
#UTA ID: 1001955622
#CSE6331
#Assignment5

from flask import Flask, flash, render_template, request, redirect
from numpy import broadcast

import pyodbc
import csv

from datetime import datetime

import requests

from settings import server, database, username, password, driver, mapQuest_key, mapQuest_url

import time


connstr = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password


from flask_socketio import SocketIO,send 
import pusher

pusher_client = pusher.Pusher(
  app_id='1388441',
  key='9e2081fcbc7cd3f0e82a',
  secret='fe1670b0495e222562c6',
  cluster='us2',
  ssl=True
)



app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins='*')

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



###### SocketIO Routes ######
@socketio.on('message')
def handle_message(msg):
    print('message:',msg)
    send(msg, broadcast=True)




###### Routes ######



@app.route("/", methods=['GET','POST'])
@app.route("/home", methods=['GET','POST'])
def home_function():

    #pusher_client.trigger('my-channel', 'my-event', {'message': 'hello world'})
    
    if request.method == 'POST':
        try:
            if 'search_51' in request.form:
                search_51 = request.form["search_51"]
                list_result51 = "search_entered_query(search_51)"
                #print(list_result51)
                count_rows51 = f"The count of documents matched is {len(list_result51)}."
                t_headings51 = ["Document Name", "Matched Lines"]

                return render_template('home.html',count_rows51=count_rows51,t_headings51=t_headings51,list_result51=list_result51)

            if 'another':
                pass










        except Exception as e:
            print(e,"Error has occured")
            

    return render_template('home.html')








######## Retrieve Data in /home ######

    #         if 'search_mag' in request.form:
    #             #print(request.form["search_name"])
    #             magnitude = request.form["search_mag"]
    #             from_date = request.form["search_from_date"]
    #             to_date = request.form["search_to_date"]
    #             #print(type(from_date),from_date)
    #             search_query = "SELECT * FROM av_asg3 WHERE 1 = 1 "
    #             if len(magnitude)!=0:
    #                 magnitude = float(magnitude)
    #                 search_query+= "AND mag > " + str(magnitude)
    #             if len(from_date) !=0  and len(to_date) !=0 :
    #                 search_query+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"
    #             #print(search_query)
    #             list_result1 = run_search_query(search_query)
    #             count_rows = f"The count of earthquakes occured with magnitude greather than {magnitude} is {(len(list_result1))} from {from_date} to {to_date}"
                
    #             t_headings1 = ["time", "latitude","longitude", "depth","mag","magType","nst","gap","dmin","rms","net","id","updated","place","type","horizontalError","depthError","magError","magNst","status","locationSource","magSource"]

    #             return render_template('home.html',count_rows=count_rows,t_headings1=t_headings1,list_result1=list_result1,scroll1="scroll1")
            
    #         if 'dist_dist' in request.form:
    #             location = request.form["dist_loc"]
    #             latitude = request.form["dist_lat"]
    #             longitude = request.form["dist_long"]
    #             from_date = request.form["dist_from_date"]
    #             to_date = request.form["dist_to_date"]
    #             if len(location)!=0:
    #                 main_url = mapQuest_url+mapQuest_key+'&location='+location
    #                 print(main_url)

    #                 location_data = requests.get(main_url).json()['results'][0]['locations'][0]['latLng']
    #                 print("location_data=",location_data)
    #                 latitude = location_data['lat']
    #                 longitude = location_data['lng']
    #                 #print("lat and lang=",latitude,longitude)

    #             distance = request.form["dist_dist"]
    #             if len(distance)!=0:
    #                 distance = float(distance)

    #             search_query = "SELECT * FROM av_asg3 WHERE 1 = 1 "
    #             if len(from_date) !=0  and len(to_date) !=0 :
    #                 search_query+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"
    #             search_query+=" ORDER By mag DESC"
    #             list_result2 = run_search_query(search_query)
    #             location1 = (float(latitude),float(longitude))
                    
    #             list_result2_updated = []
    #             for item in list_result2:
    #                 location2 = (float(item[1]),float(item[2]))
    #                 actual_diff = hs.haversine(location1,location2)
    #                 if distance >= actual_diff:
    #                     list_result2_updated.append(item)
                
    #             count_rows2 = f"The count of earthquakes from {location} lat= {latitude} and lng= {longitude} within {distance} km is {len(list_result2_updated)} from {from_date} to {to_date}"
                
    #             t_headings2 = ["time", "latitude","longitude", "depth","mag","magType","nst","gap","dmin","rms","net","id","updated","place","type","horizontalError","depthError","magError","magNst","status","locationSource","magSource"]

    #             return render_template('home.html',count_rows2=count_rows2,t_headings2=t_headings2,list_result2_updated=list_result2_updated,scroll2="scroll2")
            



















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
    socketio.run(app,host='0.0.0.0',debug=True)


