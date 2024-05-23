This package is created for CRUD actions on MongoDB databases 

The package uses Pymongo library and a default dataframe with a strict format has to be given as input for the insert actions.

The input dataframe structure should be --> DATE, val, TIME

The library can be installed as - pip install crudLib

and imported as following.

from crudLib import mongofuncs



Insert or Update actions use function : mongofuncs.insertUpdate
-----> Inputs: connection uri, database name, collection name, dataframe, UUID
usage: aa = mongofuncs.insertUpdate(uri, 'db', 'col', df, UUID('695120e3-0165-42f4-bc72-45004932a6ba'))



To remove values use function : mongofuncs.deletes
-----> Inputs: connection uri, database name, collection name, UUID, time1, time2(optional)
usage: aa = mongofuncs.deletes(uri, 'db', 'col', UUID('695120e3-0165-42f4-bc72-45004932a6ba'), time1= 1700654400)



For search and Fetch actions use function: mongofuncs.fetch
-----> Inputs: connection uri, database name, collection name, UUID, time1(optional), time2(optional)
usage: aa = mongofuncs.fetch(uri, 'db', 'col', UUID('695120e3-0165-42f4-bc72-45004932a6ba'), time1= 1609654400, time2= 1700654400)

!!!!!
