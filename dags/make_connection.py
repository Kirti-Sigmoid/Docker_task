import psycopg2


def check_connection(**kwargs):
    context = kwargs

    try:
        conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow", port='5432')
        cursor = conn.cursor()
        print ('connection successful')

        add_data = 'create table if not exists assignment_dag_table( dag_id varchar(40),' \
                   'execution_date varchar(40))'

        # insert_data="insert into assignment_dag_table(dag_id,execution_date) values (id,date)"
        cursor.execute(add_data)
        # cursor.execute(insert_data)
        conn.commit()

        print("data added to a new table Successfully")

        query = 'select * from assignment_dag_table;'
        cursor.execute(query)
        data = cursor.fetchall()
        print("This is Data Execution time for every DAG runs :- ")
        for i in data:
            print(i)
        print("Data Fetched to the Console Successfully")


    except:
        print("Error in connection")
    finally:
        conn.close()
        print("No issues")