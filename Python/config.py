# SQL Database (MSSQL) Configuration
SQL_CONFIG = {
    'driver': '/opt/homebrew/lib/libmsodbcsql.18.dylib',  # Correct driver path
    'server': 'tcp:mcruebs04.isad.isadroot.ex.ac.uk',
    'database': 'BEMM459_GroupR',
    'username': 'GroupR',
    'password': 'YgfK928+At',
    'trust_server_certificate': 'yes',
    'encrypt': 'no'
}

# config.py
MONGODB_CONFIG = {
    "username": "businessdarvesh",
    "password": "asdfghjkl123",
    "cluster": "groupr.gpmjz.mongodb.net",
    "params": "retryWrites=true&w=majority&appName=GroupR",
    "database": "Polyglot_persistense",
    "collection": "streamflix"
}