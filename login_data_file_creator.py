import json

'''in case you want to store login info json without too much hassle if youre testing this code'''
login_data = {
    'sql_host': 'localhost',
    'sql_port': '5432',
    'sql_name': 'postgres',
    'sql_user': 'postgres',
    'sql_password': 'mypassword'
}

json_object = json.dumps(login_data, indent=4)
with open('login_data.json', 'w') as fp:
    fp.write(json_object)
