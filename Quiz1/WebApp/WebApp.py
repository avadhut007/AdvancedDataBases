from flask import Flask, flash, render_template, request, redirect

import pyodbc
import csv
from azure.storage.blob import BlobServiceClient

server = 'asg1.database.windows.net'
database = 'asg1DB'
username = 'firstUser'
password = 'SaveYourDB123'   
driver= '{ODBC Driver 17 for SQL Server}'

connstr = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

blob_connstr = 'DefaultEndpointsProtocol=https;AccountName=avtstorage1;AccountKey=ADw3GwRM8/7xT942C7zA85obs7FhyKLfLIi0Se0ySOSCj3WXTwJr6zEjSRTTdmTz4kwCIjHVJiAh+AStBhRG+Q==;EndpointSuffix=core.windows.net' 
container_name = 'asg1'
blob_account_name = 'avtstorage1'

#Store credentials and variable in config file

web_app = Flask(__name__)


#SQL

def upload_csv(filename): # path not required since we will select manually
    try:
        conn = pyodbc.connect(connstr)
        #print("conn",conn)
        cursor = conn.cursor()
        #print("cursor",cursor)
        path = './'
        count = 0
        table = 'av_quiz1'
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


def upload_image(imagefile,username):
    # check if username and imagename from db matches then upload/replace it 
    # if does not match then remove exisitng image and upload new one and update in DB
    
    try:
        conn = pyodbc.connect(connstr)
        #print("conn",conn)
        cursor = conn.cursor()
        cursor.execute("SELECT name,picture FROM av_people")

        list_names = cursor.fetchall()
        list_names = dict(list_names)
        print(list_names,"type")
        
        blob_service_client = BlobServiceClient.from_connection_string(blob_connstr)
        container_client = blob_service_client.get_container_client(container_name)
        #print((username.capitalize(),imagefile.filename),(username.capitalize(),imagefile.filename) in list_names,"username") 


        if username.capitalize() in list_names.keys():
            if imagefile.filename in list_names.values():
                container_client.upload_blob(imagefile.filename, imagefile, overwrite=True)
                #replace blob if same name
            else:
                container_client.upload_blob(imagefile.filename, imagefile)
                old_image = list_names[username.capitalize()]
                for blob in container_client.list_blobs():
                    if old_image == blob.name:#check if blob exisits
                        container_client.delete_blob(old_image,delete_snapshots="include") # if it exists
                
                cursor.execute("UPDATE av_people SET picture = ? WHERE name = ?;",imagefile.filename,username)
                cursor.commit()
                # upload image and remove prev image and update picture column 
                
        else:
            raise Exception("Wrong Username")

        # remaining case what happens if blob exists for another usereg. chuck.jpg already exists with Chuck if uploaded for DAve
    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()

def search_by_name(username):
    #search name in DB get image names and search in blob and show the image
    try:
        conn = pyodbc.connect(connstr)
        #print("conn",conn)
        cursor = conn.cursor()
        cursor.execute("SELECT name,picture FROM av_people")
        list_names = cursor.fetchall()
        list_names = dict(list_names) 

        blob_service_client = BlobServiceClient.from_connection_string(blob_connstr)
        container_client = blob_service_client.get_container_client(container_name)

        if username.capitalize() in list_names.keys():
            blob_name = list_names[username.capitalize()]

            blob_url = f"https://{blob_account_name}.blob.core.windows.net/{container_name}/{blob_name}" 
            print("blob_url",blob_url)
        else:
            blob_url =''
    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()
    return blob_url

def search_by_everything(object,min_size,max_size,charm):
    try:

        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        #print(min_size,"min_size")
        search_query = "SELECT * FROM av_quiz1 WHERE 1=1 "
        if str(object) != "None" and len(object) != 0:
                search_query += " AND LOWER(object) LIKE '%"+object.lower()+"%' "
        if str(min_size) != "None" and len(min_size) != 0:
            if int(min_size) > 0:
                    search_query += " AND max  > " + str(min_size) + " AND min < " + str(min_size)
        if str(charm) != "None" and len(charm) != 0:
                search_query += " AND LOWER(charm) LIKE '%"+charm.lower()+"%' "
        
        #print("search_q",search_query)
        
        cursor.execute(search_query)
        list_names = cursor.fetchall()
        
        #print(list_names,"list_names","len=",len(list_names))
        list_names_update = []
        for item in list_names:
            print(list(item),"list(item)=") 
            item = list(item) 
            if item[3] == ' ':
                item.append('')
            else:
                item.append(f"https://{blob_account_name}.blob.core.windows.net/{container_name}/{item[3]}" )      
            list_names_update.append(item)

        #print( list_names_update,"list_names","len=",len(list_names))
    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()
    return list_names_update


def delete_profile(username):
    try:

        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM av_people WHERE name = ?;",username)
        cursor.commit()
        print("deleted profile of ",username)
    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()

def delete_picture(username):
    try:

        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute("SELECT name,picture FROM av_people")

        list_names = cursor.fetchall()
        list_names = dict(list_names)

        blob_service_client = BlobServiceClient.from_connection_string(blob_connstr)
        container_client = blob_service_client.get_container_client(container_name)

        if username.capitalize() in list_names.keys():
            image_name = list_names[username.capitalize()]

            for blob in container_client.list_blobs():         
                if image_name == blob.name:#check if blob exisits
                    container_client.delete_blob(image_name,delete_snapshots="include")
                    cursor.execute("UPDATE av_people SET picture = ' ' WHERE name = ?;",username)
                    cursor.commit()
                    print(image_name," image deleted")

    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()

def update_records(name,state,sal,grade,room,telnum,keyw):
    try:

        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        #print(min_size,"min_size")
        update_query = "UPDATE av_people SET "
        update_query += " name = '" + name + "' "
        if str(state) != "None" and len(state) != 0:
            update_query += ", state = '" + str(state) + "' "
        if str(sal) != "None" and len(sal) != 0 and int(sal) > 0:
            update_query += ", salary = " + str(sal)
        if str(grade) != "None" and len(grade) != 0 and int(grade) > 0:
            update_query += ", grade = " + str(grade)
        if str(room) != "None" and len(room) != 0 and int(room) > 0:
            update_query += ", room = " + str(room)
        if str(telnum) != "None" and len(telnum) != 0 and int(telnum) > 0:
            update_query += ", telnum = " + str(telnum)
        if str(keyw) != "None" and len(keyw) != 0:
            update_query += ", keywords = '" + keyw + "' "
        update_query += " WHERE name = '" + str(name) + "' "
        print("update_query=",update_query)
        cursor.execute(update_query)
        cursor.commit()

    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()


#Routes

@web_app.route("/", methods=['GET','POST'])
@web_app.route("/home", methods=['GET','POST'])
def home_function():
    if request.method == 'POST':
        
        if 'search_name' in request.form:
            #print(request.form["search_name"])
            img_url_search_by_name = search_by_name(request.form["search_name"])
            return render_template('home.html',img_url_search_by_name=img_url_search_by_name)

        if 'filter_name' in request.form :
            #print("filter")
            object = request.form["filter_name"]
            min_size = request.form["filter_min_sal"]
            max_size = request.form["filter_max_sal"]
            charm = request.form["filter_room"]
            # telnum = request.form["filter_telnum"]
            filtered_Info = search_by_everything(object,min_size,max_size,charm)
            t_headings = ["object","min_size","max_size","picture","charm"] # "State", "Salary", "Grade", "Room", "Telnum" ,"Picture", "Keywords", "ImageURL"]
            return render_template('home.html',t_headings=t_headings,filtered_Info=filtered_Info)
            

    return render_template('home.html')

@web_app.route("/update", methods=['GET','POST'])
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
            update_records(name,state,sal,grade,room,telnum,keyw)

    return render_template("update.html")

web_app.config['ALLOWED_IMAGE_TYPE'] = ['PNG', 'JPG', 'JPEG', 'GIF']
web_app.config['ALLOWED_CSV_TYPE'] = ['CSV']

def allowed_csv(filename):
    if not '.' in filename:
        return False
    ext = filename.rsplit('.',1)[1]

    if ext.upper() in web_app.config['ALLOWED_CSV_TYPE']:
        return True
    else:
        return False

def allowed_image(filename):
    if not '.' in filename:
        return False
    ext = filename.rsplit('.',1)[1]

    if ext.upper() in web_app.config['ALLOWED_IMAGE_TYPE']:
        return True
    else:
        return False

@web_app.route("/upload", methods=['GET','POST'] )
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

        if 'imagefile' in request.files and 'username' in request.form:
            imagefile = request.files["imagefile"]
            
            if not allowed_image(imagefile.filename):
                print('file extension not allowed')
                return render_template("upload.html")

            upload_image(imagefile,request.form["username"])

            print("image uploaded")

        if 'delete_name' in request.form:
            delete_profile(request.form["delete_name"])
            #print("profile deleted")

        if 'delete_pic' in request.form:
            delete_picture(request.form["delete_pic"])


    return render_template("upload.html")



if __name__ == '__main__':
    web_app.run(debug=True)

