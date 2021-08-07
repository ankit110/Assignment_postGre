"""
Python code to create new employees and departments table in PostgreSQL
and perform various operations like insert, show_table, update, delete, sort, select
using Python, Flask, psycopg2.
"""

from flask import Flask, render_template, request  # For API
import psycopg2 as pg2   # For PostgreSQL Connectivity to Python
import logging as lg     # For Logging

app = Flask(__name__)
lg.basicConfig(filename='postgres.log', level=lg.ERROR, format='%(asctime)s %(message)s')

def postgresql_connection():
    conn = pg2.connect(database='postgres', user='postgres', password='password')
    cur = conn.cursor()
a = postgresql_connection()


@app.route('/create_table', methods=['POST'])  # for calling the API from Postman
def create_table():
    try:
        query = "CREATE TABLE IF NOT EXISTS departments(Dep_Id SERIAL PRIMARY KEY, Dep_Name varchar(255) NOT NULL)"
        a.cur.execute(query)
        a.conn.commit()
        query = "CREATE TABLE IF NOT EXISTS employee(Emp_Id SERIAL PRIMARY KEY, Emp_Name VARCHAR (50) UNIQUE NOT NULL, Dep_Id int, FOREIGN KEY (Dep_Id) REFERENCES departments(Dep_Id));"
        a.cur.execute(query)
        a.conn.commit()
        lg.INFO('table is created.') #Logging
    except Exception as e:
        print(e)
        a.conn.close()

@app.route('/insert', methods=['POST'])
def insert():
    try:
        table_name = request.json['table_name']
        Emp_Name = request.json['Emp_Name']
        Dep_Id = request.json['Dep_Id']
        Dep_Name = request.json['Dep_Name']
        if table_name == 'employee':
            #eg. CREATE TABLE IF NOT EXISTS employee(Emp_Id SERIAL PRIMARY KEY, EmpName VARCHAR (50) UNIQUE NOT NULL, Dep_Id int, FOREIGN KEY (Dep_Id) REFERENCES departments(Dep_Id);
            query = f"INSERT INTO {table_name}(EmpName, Dep_Id) VALUES ({Emp_Name}, {Dep_Id});"
            a.cur.execute(query)
            a.conn.commit()
            lg.INFO('New Record Inserted In "EMPLOYEE" Table.')
        elif table_name == 'departments':
            #eg. CREATE TABLE if not exist departments(Dep_Id int NOT NULL AUTO_INCREMENT, Dep_Name varchar(255) NOT NULL, PRIMARY KEY(Dep_Id));
            query = f"INSERT INTO {table_name}(Dep_Name) VALUES ({Dep_Name});"
            a.cur.execute(query)
            a.conn.commit()
            lg.INFO('New Record Inserted In "DEPARTMENTS" Table.')
    except Exception as e:
        print(e)
        a.conn.close()

@app.route('/show_table', methods=['POST'])
def show_table():
    try:
        table_name = request.json['table_name']
        if table_name == 'employee':
            #eg. DELETE FROM employee WHERE Emp_Id = 1;
            query = "select * from employee;"
            a.cur.execute(query)
            a.cur.fetchall()
            lg.INFO('All Records Fetch From "EMPLOYEE" Table.')
        elif table_name == 'departments':
            query = "select * from departments;"
            a.cur.execute(query)
            a.conn.commit()
            lg.INFO('All Records Fetch From "DEPARTMENTS" Table.')
    except Exception as e:
        print(e)
        a.conn.close()

@app.route('/update', methods=['POST'])
def update():
    try:
        table_name = request.json['table_name']
        Dep_Id = request.json['Dep_Id']
        Emp_Id = request.json['Eep_Id']

        #eg. UPDATE employee SET Dep_Id = 2 WHERE Emp_Id = 1;
        query = f"UPDATE {table_name} SET Dep_Id = {Dep_Id} WHERE Emp_Id = {Emp_Id};"
        a.cur.execute(query)
        a.conn.commit()
        lg.INFO('Record Is Updated In "EMPLOYEE" Table.')
    except Exception as e:
        print(e)
        a.conn.close()

@app.route('/delete', methods=['POST'])
def delete():
    try:
        table_name = request.json['table_name']
        Emp_Id = request.json['Emp_Id']
        Dep_Id = request.json['Dep_Id']
        if table_name == 'employee':
            #eg. DELETE FROM employee WHERE Emp_Id = 1;
            query = f"DELETE FROM {table_name} WHERE Emp_Id = {Emp_Id};"
            a.cur.execute(query)
            a.conn.commit()
            lg.INFO('Record Deleted From "EMPLOYEE" Table.')
        elif table_name == 'departments':
            query = f"DELETE FROM {table_name} WHERE Dep_Id = {Dep_Id};"
            a.cur.execute(query)
            a.conn.commit()
            lg.INFO('Record Deleted From "DEPARTMENTS" Table.')
    except Exception as e:
        print(e)
        a.conn.close()

@app.route('/sort', methods=['POST'])
def sort():
    try:
        table_name = request.json['table_name']
        Emp_Id = request.json['Emp_Id']
        Dep_Id = request.json['Dep_Id']
        if table_name == 'employee':
            #eg. SELECT * FROM employee ORDER BY Emp_Id;
            query = f"SELECT * FROM {table_name} ORDER BY Emp_Id;"
            a.cur.execute(query)
            a.conn.commit()
            lg.INFO('Sort Operation Performed On "EMPLOYEE" Table.')
        elif table_name == 'departments':
            query = f"SELECT * FROM {table_name} ORDER BY Dep_Id;"
            a.cur.execute(query)
            a.conn.commit()
            lg.INFO('Sort Operation Performed On "DEPARTMENTS" Table.')
    except Exception as e:
        print(e)
        a.conn.close()

@app.route('/select', methods=['POST'])
def select():
    try:
        table_name = request.json['table_name']
        Emp_Id = request.json['Emp_Id']
        Dep_Id = request.json['Dep_Id']
        if table_name == 'employee':
            #eg. SELECT * FROM employee WHERE Emp_Id = 2;
            query = f"SELECT * FROM {table_name} WHERE Emp_Id = {Emp_Id};"
            a.cur.execute(query)
            a.conn.commit()
            lg.INFO('Select Operation Performed On "EMPLOYEE" Table.')
        elif table_name == 'departments':
            query = f"SELECT * FROM {table_name} WHERE Dep_Id = {Dep_Id};"
            a.cur.execute(query)
            a.conn.commit()
            lg.INFO('Select Operation Performed On "EMPLOYEE" Table.')
    except Exception as e:
        print(e)
        a.conn.close()

if __name__ == '__main__':
    app.run()