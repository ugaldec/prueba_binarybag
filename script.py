import mysql.connector as mysql
from mysql.connector import Error
import csv

url_archivo_base = 'data_deafio_bb.csv'
url_archivo_nuevo = 'respuesta_desafio_bb.csv'
host = "localhost"
user = "root"
password = "gupi"
dbName = 'dbPrueba' 


# crea nuevo archivo csv con los headers del nuevo formato
fieldnames = ['id', 'nombres', 'apellido_paterno', 'apellido_materno' ,'cod_entidad','cod_tramo','estado','fecha','causal']
with open(url_archivo_nuevo, 'w', newline='') as nuevo:
    writer = csv.DictWriter(nuevo, delimiter='|', fieldnames=fieldnames)
    writer.writeheader()
# lee datos y los escribe  en nuevo archivo csv
with open(url_archivo_base, newline='') as dato:
     reader = csv.DictReader(dato, delimiter='|')
     for row in reader:
         nombre_completo = row['nombre_completo'].split(' - ')
         causales = row['causales'].split(',')
         for causal in causales:
               with open(url_archivo_nuevo,'a',newline='') as nuevo:
                  writer = csv.DictWriter(nuevo, delimiter='|', fieldnames = fieldnames)
                  writer.writerow({'id':row['id'], 
                                  'nombres':nombre_completo[0], 
                                  'apellido_paterno':nombre_completo[1], 
                                  'apellido_materno':nombre_completo[2] ,
                                  'cod_entidad':row['cod_entidad'],
                                  'cod_tramo':row['cod_tramo'],
                                  'estado':row['estado'],
                                  'fecha':row['fecha'],
                                  'causal':causal
                                  })
################################
## crea db si no existe
################################
existeDB = False
try:
        db = mysql.connect(
                host = host,
                user = user,
                password = password)
        if db.is_connected():
            print("-------creacion db inicio-----------------")
            cursor = db.cursor()
            cursor.execute("SHOW DATABASES;")
            databases = cursor.fetchall() 
            print('db disponibles antes =>', databases)
            existeDB = False
            for database in databases:
                if(str(database) == "('" + dbName + "',)"):
                    existeDB = True
            if(existeDB == False):
                cursor.execute("CREATE DATABASE "+ dbName +";")

            cursor.execute("SHOW DATABASES;")
            databases = cursor.fetchall() 
            print('db disponibles despues =>', databases)

except Error as e:
    print("error mysql", e)
finally:
    if db.is_connected():
        cursor.close()
        db.close()
        print("-------creacion db fin--------------------")            
###################################
## crear tablas si no existen
###################################
existeTBeneficiarios = False
existeTCausales = False    
createTableBeneficiarios = "CREATE TABLE beneficiarios (id int(10)  NOT NULL AUTO_INCREMENT PRIMARY KEY,id_beneficiario INT(10) NOT NULL,nombres VARCHAR(255),apellido_paterno VARCHAR(80),apellido_materno VARCHAR(80),cod_entidad INT(10),cod_tramo INT(1),estado VARCHAR(10),fecha DATETIME ,id_causal INT(10))"
createTableCausales = "CREATE TABLE causales (id int(10) NOT NULL AUTO_INCREMENT PRIMARY KEY,descripcion VARCHAR(80))"
try:
    db = mysql.connect(
            host = host,
            user = user,
            password = password,
            database = dbName)
    if db.is_connected():
        print("-------creacion tablas inicio-------------")
        cursor = db.cursor()
        cursor.execute("SHOW TABLES;") 
        tables = cursor.fetchall()
        print("tablas en db antes =>", tables)
        for table in tables:
            if(str(table) == "('" + "beneficiarios" + "',)"):
                existeTBeneficiarios = True
            if(str(table) ==  "('" + "causales" + "',)"):
                existeTCausales = True

        if(existeTBeneficiarios == False):
            cursor.execute(createTableBeneficiarios)
        if(existeTCausales == False):
            cursor.execute(createTableCausales)
        
        cursor.execute("SHOW TABLES;")    
        info = cursor.fetchall()
        print("tablas en db =>", info)

except Error as e:
    print("error mysql", e)
finally:
    if db.is_connected():
        cursor.close()
        db.close()
        print("-------creacion tablas fin----------------")    
#########################################
##inserta datos tabla causales
########################################
queryCausales = "INSERT INTO causales (id,descripcion) VALUES (%s, %s)"
valuesCausales = [
            ("1", "INGRESO PROMEDIO DEL BENEFICIARIO"),
            ("2", "PAGO DEL BENEFICIO INFORMADO"),
            ("3", "SIN RECLAMO")]
try:
    db = mysql.connect(
            host = host,
            user = user,
            password = password,
            database = dbName)
    if db.is_connected():
        print("-------insert causal inicio---------------")
        cursor = db.cursor()
        cursor.execute("DELETE FROM causales;")
        cursor.executemany(queryCausales, valuesCausales)
        db.commit()
        print(cursor.rowcount, "records inserted")

except Error as e:
    print("error mysql", e)
finally:
    if db.is_connected():
        cursor.close()
        db.close()
        print("-------creacion causal fin----------------")
######################################
## insertar datos tabla beneficiarios
######################################
##funcion auxiliar 
def intnull(caracter):
    num = 0
    if(caracter != None and caracter != ""):
       num =  int(caracter)
    return num
##funcion auxiliar para insertar id de causales
def f_causal(nombre , causales):
    for causal in causales:
        if(causal[1] == nombre):
            return causal[0]
    return ""

queryBeneficiarios = "INSERT INTO beneficiarios (id_beneficiario,nombres,apellido_paterno,apellido_materno,cod_entidad,cod_tramo,estado,fecha,id_causal) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
try:
    db = mysql.connect(
            host = "localhost",
            user = "root",
            password = "gupi",
            database = dbName)
    if db.is_connected():
        print("-------insert beneficiarios inicio--------")
        cursor = db.cursor()
        cursor.execute("DELETE FROM beneficiarios;")
        cursor.execute("SELECT * FROM causales")
        causales = cursor.fetchall()
        with open(url_archivo_nuevo, newline='') as dato:
            reader = csv.DictReader(dato, delimiter='|')
            total = 0
            for row in reader:
                valuesBeneficiarios =  (int(row['id']), 
                                        row['nombres'], 
                                        row['apellido_paterno'], 
                                        row['apellido_materno'], 
                                        intnull(row['cod_entidad']), 
                                        intnull(row['cod_tramo']), 
                                        row['estado'],
                                        row['fecha'], 
                                        f_causal(row['causal'],causales))
                cursor.execute(queryBeneficiarios,valuesBeneficiarios )
                total = total + cursor.rowcount
        db.commit()
        print(total, "records inserted")

except Error as e:
    print("error mysql", e)
finally:
    if db.is_connected():
        cursor.close()
        db.close()
        print("-------insert beneficiarios fin-----------")   


# 1.- Debe separar el nombre completo en sus respectivos atributos (nombres - apellido paterno - apellido materno)
# 2.- Debe cargar las causales en una tabla independiente relacionada a la de beneficiarios
# 3.- Debe almacenar la fecha como tipo date

# se debe instalar https://dev.mysql.com/downloads/file/?id=502637
# cambiar privilegios:
# ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root';
         


            


    
