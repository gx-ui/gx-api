T_ORM= {
'connections': {
            'default': {
                'engine': 'tortoise.backends.asyncpg',
                'credentials': {
                    'host': '127.0.0.1',
                    'port': '5432',
                    'user': 'postgres',
                    "password": '123456',
                    'database': 'Userdatabase',
                    "minsize": 1,
                    "maxsize": 10,
                },
            }},
        'apps': {
        "models": {
            "models": ["api.models", "aerich.models"],
            "default_connection": "default"
         }
        }


}