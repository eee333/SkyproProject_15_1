#

import sqlite3


def all_animals():
    con = sqlite3.connect("animal.db")
    cur = con.cursor()
    sqlite_query = ('''SELECT * FROM animals ''')
    cur.execute(sqlite_query)
    result = cur.fetchall()
    con.close()
    return result


# List of all tables in DB
def print_tables():
    con = sqlite3.connect("animal.db")
    cur = con.cursor()
    sqlite_query = ('''
        select * from sqlite_master
        where type = 'table'
    ''')
    cur.execute(sqlite_query)
    tables = cur.fetchall()
    print("Tables in animal.db:")
    for table in tables:
        print(table[1])  # названия таблиц
    con.close()


# Create 8 tables
def create_tables():
    con = sqlite3.connect("animal.db")
    cur = con.cursor()
    sqlite_query = ('''
        CREATE TABLE animal_type(
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        animal_type VARCHAR(50))
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        CREATE TABLE breed(
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        breed VARCHAR(200))
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        CREATE TABLE color(
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        color VARCHAR(50))
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        CREATE TABLE outcome_subtype(
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        outcome_subtype VARCHAR(50))
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        CREATE TABLE outcome_type(
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        outcome_type VARCHAR(50))
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        CREATE TABLE animals_2(
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(50),
        animal_id_old VARCHAR(10),
        date_of_birth DATE,
        animal_type_id INT,
        breed_id INT,
        FOREIGN KEY (animal_type_id) REFERENCES animal_type (id) ON DELETE CASCADE,
        FOREIGN KEY (breed_id) REFERENCES breed (id) ON DELETE RESTRICT)
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        CREATE TABLE outcome(
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        animal_id INT,
        age_upon_outcome VARCHAR(50),
        outcome_month INT,
        outcome_year INT,
        outcome_subtype_id INT,
        outcome_type_id INT,
        FOREIGN KEY (animal_id) REFERENCES animals_2 (id) ON DELETE RESTRICT,
        FOREIGN KEY (outcome_subtype_id) REFERENCES outcome_subtype (id) ON DELETE RESTRICT,
        FOREIGN KEY (outcome_type_id) REFERENCES outcome_type (id) ON DELETE RESTRICT)
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        CREATE TABLE animal_color(
        animal_id INT,
        color_id INT,
        FOREIGN KEY (animal_id) REFERENCES animals_2 (id) ON DELETE CASCADE,
        FOREIGN KEY (color_id) REFERENCES color (id) ON DELETE RESTRICT) 
    ''')
    cur.execute(sqlite_query)

    con.close()


# Fill tables
def fill_tables():
    con = sqlite3.connect("animal.db")
    cur = con.cursor()
    sqlite_query = ('''
        INSERT INTO animal_type (animal_type) SELECT DISTINCT animal_type FROM animals 
        WHERE animal_type NOTNULL 
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        INSERT INTO breed (breed) SELECT DISTINCT breed FROM animals 
        WHERE breed NOTNULL 
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        INSERT INTO color (color)
        SELECT TRIM(color1) FROM animals WHERE color1 NOTNULL 
        UNION SELECT TRIM(color2) FROM animals WHERE color2 NOTNULL 
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        INSERT INTO outcome_subtype (outcome_subtype) SELECT DISTINCT outcome_subtype FROM animals 
        WHERE outcome_subtype NOTNULL
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        INSERT INTO outcome_type (outcome_type) SELECT DISTINCT outcome_type FROM animals 
        WHERE outcome_type NOTNULL
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        INSERT INTO animals_2 (name, animal_id_old, date_of_birth, animal_type_id, breed_id)
        SELECT DISTINCT name, animal_id, date_of_birth, animal_type.id, breed.id
        FROM animals
        LEFT JOIN animal_type ON animals.animal_type = animal_type.animal_type
        LEFT JOIN breed ON animals.breed = breed.breed
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        INSERT INTO outcome (animal_id, age_upon_outcome, outcome_month, outcome_year, outcome_subtype_id, outcome_type_id)
        SELECT animals_2.id, age_upon_outcome, outcome_month, outcome_year, outcome_subtype.id, outcome_type.id
        FROM animals
        LEFT JOIN animals_2 ON animals.animal_id = animals_2.animal_id_old
        LEFT JOIN outcome_subtype ON animals.outcome_subtype = outcome_subtype.outcome_subtype
        LEFT JOIN outcome_type ON animals.outcome_type = outcome_type.outcome_type
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        INSERT INTO animal_color (animal_id, color_id)
        SELECT DISTINCT animals_2.id, color.id FROM animals_2 
        JOIN animals ON animals.animal_id = animals_2.animal_id_old 
        JOIN color ON color.color = TRIM(animals.color1)
    ''')
    cur.execute(sqlite_query)

    sqlite_query = ('''
        INSERT INTO animal_color (animal_id, color_id)
        SELECT DISTINCT animals_2.id, color.id FROM animals_2 
        JOIN animals ON animals.animal_id = animals_2.animal_id_old 
        JOIN color ON color.color = TRIM(animals.color2)
    ''')
    cur.execute(sqlite_query)

    con.commit()
    con.close()


# Clear table and reset PRIMARY KAY
def clear_table(table_name):
    con = sqlite3.connect("animal.db")
    cur = con.cursor()
    sqlite_query = (f'''
        DELETE FROM {table_name}
    ''')
    cur.execute(sqlite_query)
    sqlite_query = (f'''
        UPDATE SQLITE_SEQUENCE SET seq = 0 WHERE name = '{table_name}'
    ''')
    cur.execute(sqlite_query)
    con.commit()
    con.close()


# Delete table
def del_table(table_name):
    con = sqlite3.connect("animal.db")
    cur = con.cursor()
    sqlite_query = (f'''
        DROP TABLE {table_name}
    ''')
    cur.execute(sqlite_query)
    con.commit()
    con.close()


create_tables()
# print_tables()
fill_tables()
# del_table('animals')

