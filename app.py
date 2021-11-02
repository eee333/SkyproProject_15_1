from flask import Flask, request, Response
import sqlite3
import json


def query_db(sqlite_query): # Execute query, return list of dict
    with sqlite3.connect("animal.db") as connection:
        cursor = connection.cursor()
        cursor.execute(sqlite_query)
        fields = [description[0] for description in cursor.description]
        result = []
        for item in cursor.fetchall():
            line = dict(zip(fields, item))
            result.append(line)
        return result


app = Flask(__name__)


@app.route('/<int:itemid>/')
def rating_children(itemid):
    status = '200'
    result = query_db(f'''
        SELECT animals_2.id, name, animal_type, breed, date_of_birth
        FROM animals_2
        LEFT JOIN animal_type ON animals_2.animal_type_id = animal_type.id
        LEFT JOIN breed ON animals_2.breed_id = breed.id
        WHERE animals_2.id = {itemid}
    ''')
    if not result:
        result = {"error": "Not found"}
        status = '404'
    body = json.dumps(result)

    response = Response(body, content_type='application/json', status=status)
    return response


if __name__ == '__main__':
    app.run()