#Name: Avadhut Vijay Talbar
#UTA ID: 1001955622
#CSE6331
#Assignment2

from flask import Flask, flash, render_template, request, redirect

import pyodbc
import csv

from datetime import datetime
import haversine as hs
import requests
import numpy as np

server = 'asg1.database.windows.net'
database = 'asg1DB'
username = 'firstUser'
password = 'SaveYourDB123'   
driver= '{ODBC Driver 17 for SQL Server}'

connstr = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

mapQuest_key = 'b0HyfKesjA9DnqNox19MAAWmjb18rdwH'
mapQuest_url = 'http://open.mapquestapi.com/geocoding/v1/address?key='

#Store credentials and variable in config file

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
        #print(list_result,"type")

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
        print(list_result,"type")

    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()
    return list_result


#Routes

@app.route("/", methods=['GET','POST'])
@app.route("/home", methods=['GET','POST'])
def home_function():
    if request.method == 'POST':
        try:

            if 'task10_lat' in request.form:
                task10_lat = float(request.form["task10_lat"])
                task10_deg = float(request.form["task10_deg"])
                search_query = "SELECT time, latitude, longitude, id, place FROM av_quiz2 WHERE latitude between '" + str(task10_lat-task10_deg) + "' AND '" + str(task10_lat+task10_deg) + "'"
                print(search_query)
                list_result10 = run_search_query(search_query)
                #print(list_result10)

                count_rows10 = f"The earthquake between {task10_lat} - {task10_deg} and {task10_lat} + {task10_deg} is below "
                t_headings10 = ["time", "latitude","longitude", "id","place"]

                return render_template('home.html',count_rows10=count_rows10,t_headings10=t_headings10,list_result10=list_result10)
            
            if 'task12_from_mag' in request.form:
                task12_from_mag = float(request.form["task12_from_mag"])
                task12_to_mag = float(request.form["task12_to_mag"])
                task12_place = request.form["task12_place"]
                search_query = "SELECT time, latitude, longitude, mag, id, place FROM av_quiz2 WHERE (mag between '" + str(task12_from_mag) + "' AND '" + str(task12_to_mag) + "') AND place like '%" + str(task12_place) + "%'"
                print(search_query)
                list_result12 = run_search_query(search_query)
                #print(list_result10)

                #count_rows10 = f"The earthquake between {task10_lat} - {task10_deg} and {task10_lat} + {task10_deg} is below "
                t_headings12 = ["time", "latitude","longitude", "mag", "id","place"]

                return render_template('home.html',t_headings12=t_headings12,list_result12=list_result12)
            

            if 'task13_type' in request.form:
                task13_type = request.form["task13_type"]
                task13_net = request.form["task13_net"]
                
                search_query = "SELECT time, latitude, longitude, mag, id, place FROM av_quiz2 WHERE LOWER(type) like '%" + str(task13_type.lower()) + "%'  AND LOWER(net) like '%" + str(task13_net.lower()) + "%'"
                print(search_query)
                list_result13 = run_search_query(search_query)
                print(list_result13)
                count = len(list_result13)
                count_rows13 = f"The count is {count} "
                

                return render_template('home.html',count_rows13=count_rows13)














            if 'search_mag' in request.form:
                #print(request.form["search_name"])
                magnitude = request.form["search_mag"]
                from_date = request.form["search_from_date"]
                to_date = request.form["search_to_date"]
                print(type(from_date),from_date)
                search_query = "SELECT * FROM av_asg2 WHERE 1 = 1 "
                if len(magnitude)!=0:
                    magnitude = float(magnitude)
                    search_query+= "AND mag > " + str(magnitude)
                if len(from_date) !=0  and len(to_date) !=0 :
                    search_query+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"
                #print(search_query)
                list_result1 = run_search_query(search_query)
                count_rows = f"The count of earthquakes occured with magnitude greather than {magnitude} is {(len(list_result1))} from {from_date} to {to_date}"
                
                t_headings1 = ["time", "latitude","longitude", "depth","mag","magType","nst","gap","dmin","rms","net","id","updated","place","type","horizontalError","depthError","magError","magNst","status","locationSource","magSource"]

                return render_template('home.html',count_rows=count_rows,t_headings1=t_headings1,list_result1=list_result1)
            
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

                search_query = "SELECT * FROM av_asg2 WHERE 1 = 1 "
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

                return render_template('home.html',count_rows2=count_rows2,t_headings2=t_headings2,list_result2_updated=list_result2_updated)
            
            if 'group_from_date' in request.form:
                group_from_date = request.form["group_from_date"]
                group_to_date = request.form["group_to_date"]
                list_result3_updated = []

                for i in np.arange(3,5,0.1):
                    print(round(i,2),round(i+0.1,2))

                for i in range(1,7):
                    search_query = "SELECT * FROM av_asg2 WHERE mag BETWEEN " + str(float(i)) + " AND " + str(float(i+1)) + " AND time between '" + str(group_from_date) + "' AND '" + str(group_to_date) + "'"
                    list_result3 = run_search_query(search_query)
                    #print(i,i+1,"result",list_result3)
                    list_result3_updated.append([(i,i+1),len(list_result3)])
                #print("result updated",list_result3_updated)

                count_rows3 = f"The No of earthquakes from {group_from_date} to {group_to_date} are below"
                t_headings3 = ["Magnitude Group","No of Earthquakes"]
                return render_template('home.html',scroll1="mag_group",count_rows3=count_rows3,t_headings3=t_headings3,list_result3_updated=list_result3_updated)


            if 'no_of_quakes' in request.form:
                no_of_quakes =  request.form["no_of_quakes"]
                search_query = "SELECT TOP " +no_of_quakes+ " * FROM av_asg2 ORDER BY mag DESC"
                list_result4 = run_search_query(search_query)

                count_rows4 = f"The top {no_of_quakes} Earthquakes are as below"
                t_headings4 = ["time", "latitude","longitude", "depth","mag","magType","nst","gap","dmin","rms","net","id","updated","place","type","horizontalError","depthError","magError","magNst","status","locationSource","magSource"]

                return render_template('home.html',scroll2="largest_quakes",count_rows4=count_rows4,t_headings4=t_headings4,list_result4=list_result4)
     
        except Exception as e:
            print(e,"Error has occured")
            

    return render_template('home.html')

@app.route("/update", methods=['GET','POST'])
def update_function():
    if request.method == 'POST':
        if 'update_name' in request.form :
            name = request.form["update_name"]
            state = request.form["update_state"]
            sal = request.form["update_sal"]
            grade = request.form["update_grade"]
            room = request.form["update_room"]
            telnum = request.form["update_telnum"]
            keyw = request.form["update_keyw"]
            #update_records(name,state,sal,grade,room,telnum,keyw)

    return render_template("update.html")

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

