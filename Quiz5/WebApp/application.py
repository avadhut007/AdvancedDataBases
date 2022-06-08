#Name: Avadhut Vijay Talbar
#UTA ID: 1001955622
#CSE6331
#Assignment5

from flask import Flask, flash, render_template, request, redirect

import pyodbc
import csv

from datetime import datetime

import requests

from settings import server, database, username, password, driver, mapQuest_key, mapQuest_url

import time


connstr = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password




application = Flask(__name__)


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







###### Text Search Functions ######

import os
import sys
from whoosh.index import create_in 
from whoosh.fields import Schema, TEXT, ID
from whoosh.analysis import StemmingAnalyzer

my_analyzer = StemmingAnalyzer() 

def read_text_docs(root_dir):   
    schema = Schema(title=TEXT(stored=True),path=ID(stored=True), content=TEXT(analyzer=my_analyzer), textdata=TEXT(stored=True))
    if not os.path.exists("indexdir"):
        os.mkdir("indexdir")
        
    index_sch = create_in("indexdir",schema)
    writer = index_sch.writer()
    
    filepaths = [os.path.join(root_dir,i) for i in os.listdir(root_dir)]
    for filepath in filepaths:
        fp = open(filepath,'r',encoding='utf-8')
        print(filepath)
        text = fp.read()
        writer.add_document(title=filepath.split("/")[1], path=filepath,content=text, textdata=text)
        fp.close()        
    writer.commit()

root_dir = "SearchDocuments"
read_text_docs(root_dir)

from whoosh.index import open_dir
from whoosh.qparser import QueryParser
from whoosh import scoring,highlight,qparser

def search_entered_query(query_str):
    index_sch = open_dir("indexdir")
    list_result51 = []
    with index_sch.searcher(weighting=scoring.BM25F) as searcher:
            query_parser = qparser.QueryParser("content", index_sch.schema)
            query_parser.add_plugin(qparser.FuzzyTermPlugin())
            query = query_parser.parse(query_str)
            results = searcher.search(query, terms=True, limit=None)
            results.fragmenter = highlight.SentenceFragmenter(charlimit = None)

            for i in range(len(results)):
                with open(results[i]["path"],'r',encoding='utf-8') as fileobj:
                    filecontents = fileobj.read()
                list_result51.append([results[i]['title'],(results[i].highlights("content", text=filecontents )).replace('...','<br>') ])
    return list_result51      







###### Routes ######

@application.route("/", methods=['GET','POST'])
@application.route("/home", methods=['GET','POST'])
def home_function():
    if request.method == 'POST':
        try:

            if 'search_51' in request.form:
                search_51 = request.form["search_51"]
                list_result51 = search_entered_query(search_51)
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
@application.route("/update", methods=['GET','POST'])
def update_function():
    if request.method == 'POST':
        if 'update_name' in request.form :
            pass

    return render_template("update.html")

######### UPLOAD PAGE #########

application.config['ALLOWED_CSV_TYPE'] = ['CSV']

def allowed_csv(filename):
    if not '.' in filename:
        return False
    ext = filename.rsplit('.',1)[1]

    if ext.upper() in application.config['ALLOWED_CSV_TYPE']:
        return True
    else:
        return False

@application.route("/upload", methods=['GET','POST'] )
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
    application.run(host='0.0.0.0',debug=True)
    
