This package is created for CRUD actions on MongoDB databases 

The package uses Pymongo library and a default dataframe with a strict format has to be given as input for the insert actions.

The input dataframe structure should be --> DATE, val, TIME

The library can be installed as - pip install git+https://github.com/imgitmen/hielen_mongodb_connector.git

and imported as following.

import crudLib

or 

from crudLib import mongofuncs

For all functions if 'uri' doesnot contain authentication data (username and password), Please provide them using **kwargs as 'user' = 'username', 'pw' = 'password' inorder to avoid unauthorized transactions.

****************************************************************************************************************************************************************************************************************************************

Insert or Update actions use function : mongofuncs.insertUpdate
-----> Inputs: connection uri, database name, collection name, dataframe, UUID, user, password


usage: aa = mongofuncs.insertUpdate(uri, 'db', 'col', df, UUID('695120e3-0165-42f4-bc72-45004932a6ba'), **kwargs)


***************************************************************************************************************************************************************************************************************************************

time1:time2 enter value in range format as to follow operations between "$gte time1:$lte time2" 

To remove/delete values use function : mongofuncs.deletes
-----> Inputs: connection uri, database name, collection name, UUID, time1(mandatory), user, password, time2(optional)


usage: aa = mongofuncs.deletes(uri, 'db', 'col', UUID('695120e3-0165-42f4-bc72-45004932a6ba'), time1= 1700654400, **kwargs)


***************************************************************************************************************************************************************************************************************************************

For search and Fetch actions use function: mongofuncs.fetch
-----> Inputs: connection uri, database name, collection name, UUID, user, password, time1(optional), time2(optional)

usage: aa = mongofuncs.fetch(uri, 'db', 'col', UUID('695120e3-0165-42f4-bc72-45004932a6ba'), time1= 1609654400, time2= 1700654400, **kwargs)


**************************************************************************************************************************************************************************************************************************************
The insertUpdate and deletes functions engages with transactions, which enables atomicity. i.e., either all the steps of the process is successfully carried out or the session is aborted and no changes will reflect on the database.

!!!!!


The script can be easily updated to tls/ssl connections (see commented lines in script 'mongofuncs.py').
