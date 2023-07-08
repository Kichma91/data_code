import json


def create_login_file():
    """in case you want to store login info json without too much hassle if youre testing this code"""
    config = {
        'sql_host': 'localhost',
        'sql_port': '5432',
        'sql_name': 'postgres',
        'sql_user': 'postgres',
        'sql_password': 'mypassword',
        'space_nk_update': True,
        'space_nk_sheets': None

    }

    json_object = json.dumps(config, indent=4)
    with open('../config.json', 'w') as fp:
        fp.write(json_object)


if __name__ == '__main__':
    create_login_file()




