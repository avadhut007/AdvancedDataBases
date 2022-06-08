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

from settings import server, database, username, password, driver, mapQuest_key, mapQuest_url

import time


connstr = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password




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

def decrange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step


###### Routes ######

@app.route("/", methods=['GET','POST'])
@app.route("/home", methods=['GET','POST'])
def home_function():
    if request.method == 'POST':
        try:

            if 'nvalue_45' in request.form:
                nvalue_45 = int(request.form["nvalue_45"])

                diff = (211-8)/nvalue_45
                list_result45_updated = []
                intervals = decrange(8,212,diff)
                slices = 0
                for i in intervals:
                    slices+=1
                    search_query = "SELECT count(*) FROM quiz4 WHERE col3 BETWEEN " + str(i) + " AND " + str((i+diff))
                    print(search_query)
                    list_result45 = run_search_query(search_query)
                    #print(i,i+1,"result",list_result3)
                    list_result45_updated.append([f"{i} to {i+diff}",list_result45[0][0]])
                    if slices == nvalue_45:
                        break
                print("result updated",list_result45_updated)


                return render_template('piechart.html',data_for_chart=list_result45_updated)

            if 'low_46' in request.form:
                low_46 = float(request.form["low_46"])
                high_46 = float(request.form["high_46"])
                nvalue_46 = int(request.form["nvalue_46"])

                diff = (high_46-low_46)/nvalue_46
                list_result46_updated = []
                intervals = decrange(low_46,high_46+1,diff)
                slices = 0
                for i in intervals:
                    slices+=1
                    search_query = "SELECT count(*) FROM quiz4 WHERE col2 BETWEEN " + str(i) + " AND " + str((i+diff))
                    print(search_query)
                    list_result46 = run_search_query(search_query)
                    #print(i,i+1,"result",list_result3)
                    list_result46_updated.append([f"{i} to {i+diff}",list_result46[0][0]])
                    if slices == nvalue_46:
                        break
                print("result updated",list_result46_updated)


                return render_template('barchart.html',data_for_chart=list_result46_updated)


            if 'low_47' in request.form:
                low_47 = float(request.form["low_47"])
                high_47 = float(request.form["high_47"])


                list_result47_updated = []


                search_query = "SELECT * FROM quiz4 WHERE col3 BETWEEN " + str(low_47) + " AND " + str(high_47)
                print(search_query)
                list_result47 = run_search_query(search_query)
                search_query = "SELECT col1*col2 FROM quiz4 WHERE col3 BETWEEN " + str(low_47) + " AND " + str(high_47)
                print(search_query)
                list_result48 = run_search_query(search_query)
                print(list_result48)
                #print(i,i+1,"result",list_result3)
                for i in range(len(list_result47)):
                    list_result47_updated.append([list_result47[i][0],list_result48[i][0]])

                print("result updated",list_result47_updated)


                return render_template('scatterchart.html',data_for_chart=list_result47_updated)















            if 's_mag_range_41' in request.form:
                s_mag_range_41 = int(request.form["s_mag_range_41"])
                e_mag_range_41 = int(request.form["e_mag_range_41"])
                step_41 = int(request.form["step_41"])

                list_result41_updated = []
                
                for i in range(s_mag_range_41,e_mag_range_41,step_41):
                    search_query = "SELECT count(*) FROM av_asg4 WHERE mag BETWEEN " + str(float(i)) + " AND " + str(float(i+step_41))
                    list_result41 = run_search_query(search_query)
                    #print(i,i+1,"result",list_result3)
                    list_result41_updated.append([f"{i} to {i+step_41}",list_result41[0][0]])
                print("result updated",list_result41_updated)


                return render_template('piechart.html',data_for_chart=list_result41_updated)
        
            if 's_mag_range_42' in request.form:
                s_mag_range_42 = int(request.form["s_mag_range_42"])
                e_mag_range_42 = int(request.form["e_mag_range_42"])
                step_42 = int(request.form["step_42"])

                list_result42_updated = []
                
                for i in range(s_mag_range_42,e_mag_range_42,step_42):
                    search_query = "SELECT count(*) FROM av_asg4 WHERE mag BETWEEN " + str(float(i)) + " AND " + str(float(i+step_42))
                    list_result42 = run_search_query(search_query)
                    #print(i,i+1,"result",list_result3)
                    list_result42_updated.append([f"{i} to {i+step_42}",list_result42[0][0]])
                print("result updated",list_result42_updated)


                return render_template('barchart.html',data_for_chart=list_result42_updated)


            if 'recent_quake_43' in request.form:
                recent_quake_43 = int(request.form["recent_quake_43"])

                list_result43_updated = []
                search_query = "SELECT TOP "+ str(recent_quake_43) +" mag,depth FROM av_asg4 order by time desc"
                list_result43 = run_search_query(search_query)
                #print(list_result43)
                for pair in list_result43:
                    list_result43_updated.append([pair[0],pair[1]])
                print("result updated",list_result43_updated)


                return render_template('scatterchart.html',data_for_chart=list_result43_updated)

            if 'histogram_44' in request.form:
                list_result44_updated = []
                search_query = "SELECT mag FROM av_asg4 WHERE 1=1"
                list_result44 = run_search_query(search_query)
                #print(list_result44)
                for i in list_result44:
                    list_result44_updated.append([i[0]])
                #print("result updated",list_result44_updated)


                return render_template('histogram.html',data_for_chart=list_result44_updated)








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

