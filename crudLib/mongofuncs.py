import uuid, glob, os, sys
import pandas as pd
from datetime import datetime
from uuid import UUID
from bson.objectid import ObjectId
from pymongo import MongoClient, UpdateOne
from pprint import pprint

def connecting(uri, db, col):
    client = MongoClient(uri, uuidRepresentation='standard')
    database = client[db]
    collection = database[col]
    return client, collection
def insertUpdate(uri, db, col, df, uui):
    client, coll = connecting(uri, db, col)
    df1 = pd.DataFrame(df)
    dd = df1['DATE'].sort_values().head(1).reset_index()
    date = int(dd['DATE'].iloc[0])
    dd = list(df1['DATE']+df1['TIME'])
    df1['DATE'] = date
    df1['ts'] = dd - df1['DATE']
    df1 = df1.drop('TIME', axis=1)
    mintime = min(dd)
    maxtime = max(dd)
    pipe = [{"$match": {"uuid": uui}},\
            {"$addFields": \
            {"ttt" :{"$arrayElemAt": [ "$ts_val", 0]},"ts": {"$arrayElemAt": [ "$ts_val", 0]}, "val": {"$arrayElemAt": [ "$ts_val", -1]}}},\
            {"$unwind": "$ts"},\
            {"$addFields": \
            {"time": {"$add":["$ts", "$Date"]}, "index": {"$indexOfArray": ["$ttt", "$ts"]}}},\
            {"$addFields":{"value": {"$arrayElemAt": ["$val", "$index"]}}},\
            {"$match": { "time": {'$gte': mintime, '$lte': maxtime}}},\
            {"$project": {"uuid": 0, "Date": 0, "ts_val": 0, "ts" : 0, "val": 0, "ttt" : 0}}]
    res = pd.DataFrame(list(coll.aggregate(pipe)), columns = ['_id','time', 'index', 'value'])
    timelist = list(res['time'])
    ins = []
    updates = []
    countupd = 0
    for i in dd:
        if i in timelist:
           df_index = dd.index(i)
           ind = timelist.index(i)
           objID = res['_id'].iloc[ind]
           ts_index = res['index'].iloc[ind]
           updates.append(UpdateOne({'_id': objID}, {'$set': {'ts_val.1.'+str(ts_index): df1['val'].iloc[df_index]}}))
           countupd += 1
        else:
           ins.append(dd.index(i))
    if len(updates)>0:
        coll.bulk_write(updates)
    else:
        pass
    #creating chunks of 1kb per each write
    if countupd==0:
        df_ins = df1
       # print(df_ins)
    elif len(ins)>0:
        df_ins = pd.DataFrame(df1.iloc[i,:] for i in ins).reset_index()
       # print(df_ins)
    else:
        df_ins = pd.DataFrame(columns=['DATE', 'val', 'ts'])
##    print(df_ins)
    ls = [uui, date]
    data = pd.DataFrame(columns=['uuid', 'Date', 'ts_val'])
    ts = []
    val = []
    m = 0
    for i in range(0, len(df_ins)):
            k = 800
            r = int(df_ins['ts'].iloc[i])
            v = float(df_ins['val'].iloc[i])
            k -= m
            if (k <= 0 and i != len(df_ins)-1):
               ls.append([ts, val])
               data = pd.concat([data, pd.DataFrame([ls], columns = ['uuid', 'Date', 'ts_val'])], ignore_index=True)
               ts = [r]
               val = [v]
               ls = [uui, date]
               m = sys.getsizeof(r)
            elif(i == len(df_ins)-1):
               ts.append(r)
               val.append(v)
               ls.append([ts, val])
               data = pd.concat([data, pd.DataFrame([ls], columns = ['uuid', 'Date', 'ts_val'])], ignore_index=True)
            else:
               ts.append(r)
               m += sys.getsizeof(r)
               val.append(v)
    if not data.empty:
            convert_dict = data.to_dict(orient="records")
            coll.insert_many(convert_dict)
            print('Total data count: '+str(len(df1))+'\n'+'Number of MongoDB Inserts: '+str(len(data))+\
              '\n'+'number of updates done: '+ str(countupd)+'\n'+'number of inserts done: '+str(len(ins))+\
              '\n'+'Each document has "ts, val" fields of len: '+str(len(data['ts_val'][0][0]))+\
              '\n'+'Except The last document of insert has: '+str(len(list(data['ts_val'])[-1][0])))
    else:
            print('Total data count: '+str(len(df1))+'\n'+'Number of MongoDB Inserts: '+str(len(data))+\
              '\n'+'number of updates done: '+ str(countupd)+'\n'+'No insert action took place')
def fetch(uri, db, col, uui, **kwargs):
    client, coll = connecting(uri, db, col)
    TIme1 = kwargs.get("time1")
    TIme2 = kwargs.get("time2")
    if (TIme1 is not None and TIme2 is not None):
        pipe = [{"$match": {"uuid": uui}},\
            {"$addFields": \
            {"ttt" :{"$arrayElemAt": [ "$ts_val", 0]},"ts": {"$arrayElemAt": [ "$ts_val", 0]}, "val": {"$arrayElemAt": [ "$ts_val", -1]}}},\
            {"$unwind": "$ts"},\
            {"$addFields": \
            {"time": {"$add":["$ts", "$Date"]}, "index": {"$indexOfArray": ["$ttt", "$ts"]}}},\
            {"$addFields":{"value": {"$arrayElemAt": ["$val", "$index"]}}},\
            {"$match": { "time": {'$gte': TIme1, '$lte': TIme2}}},\
            {"$project": {"_id": 0,"uuid": 0, "Date": 0, "ts_val": 0, "ts" : 0, "val": 0, "ttt" : 0, "index" : 0}}]
    elif TIme1 is not None:
        pipe = [{"$match": {"uuid": uui}},\
            {"$addFields": \
            {"ttt" :{"$arrayElemAt": [ "$ts_val", 0]},"ts": {"$arrayElemAt": [ "$ts_val", 0]}, "val": {"$arrayElemAt": [ "$ts_val", -1]}}},\
            {"$unwind": "$ts"},\
            {"$addFields": \
            {"time": {"$add":["$ts", "$Date"]}, "index": {"$indexOfArray": ["$ttt", "$ts"]}}},\
            {"$addFields":{"value": {"$arrayElemAt": ["$val", "$index"]}}},\
            {"$match": { "time": {'$gte': TIme1}}},\
            {"$project": {"_id": 0, "uuid": 0, "Date": 0, "ts_val": 0, "ts" : 0, "val": 0, "ttt" : 0, "index" : 0}}]
    elif TIme2 is not None:
        pipe = [{"$match": {"uuid": uui}},\
            {"$addFields": \
            {"ttt" :{"$arrayElemAt": [ "$ts_val", 0]},"ts": {"$arrayElemAt": [ "$ts_val", 0]}, "val": {"$arrayElemAt": [ "$ts_val", -1]}}},\
            {"$unwind": "$ts"},\
            {"$addFields": \
            {"time": {"$add":["$ts", "$Date"]}, "index": {"$indexOfArray": ["$ttt", "$ts"]}}},\
            {"$addFields":{"value": {"$arrayElemAt": ["$val", "$index"]}}},\
            {"$match": { "time": {'$lte': TIme2}}},\
            {"$project": {"_id": 0, "uuid": 0, "Date": 0, "ts_val": 0, "ts" : 0, "val": 0, "ttt" : 0, "index" : 0}}]
    else:
        pipe = [{"$match": {"uuid": uui}},\
            {"$addFields": \
            {"ttt" :{"$arrayElemAt": [ "$ts_val", 0]},"ts": {"$arrayElemAt": [ "$ts_val", 0]}, "val": {"$arrayElemAt": [ "$ts_val", -1]}}},\
            {"$unwind": "$ts"},\
            {"$addFields": \
            {"time": {"$add":["$ts", "$Date"]}, "index": {"$indexOfArray": ["$ttt", "$ts"]}}},\
            {"$addFields":{"value": {"$arrayElemAt": ["$val", "$index"]}}},\
            {"$project": {"_id": 0, "uuid": 0, "Date": 0, "ts_val": 0, "ts" : 0, "val": 0, "ttt" : 0, "index" : 0}}]
    res = pd.DataFrame(list(coll.aggregate(pipe)))
    res.rename(columns = {'value': uui, 'time':'Timestamp'},inplace = True)
    return res

def deletes(uri, db, col, uui, time1, **kwargs):
    client, coll = connecting(uri, db, col)
    TIme2 = kwargs.get("time2")
    if TIme2 is not None:
        pipe = [{"$match": {"uuid": uui}},\
            {"$addFields": \
            {"ttt" :{"$arrayElemAt": [ "$ts_val", 0]},"ts": {"$arrayElemAt": [ "$ts_val", 0]}, "val": {"$arrayElemAt": [ "$ts_val", -1]}}},\
            {"$unwind": "$ts"},\
            {"$addFields": \
            {"time": {"$add":["$ts", "$Date"]}, "index": {"$indexOfArray": ["$ttt", "$ts"]}}},\
            {"$addFields":{"value": {"$arrayElemAt": ["$val", "$index"]}}},\
            {"$match": { "time": {'$gte': time1, '$lte': TIme2}}},\
            {"$project": {"uuid": 0, "Date": 0, "ts_val": 0, "ts" : 0, "val": 0, "ttt" : 0}}]
    else:
        pipe = [{"$match": {"uuid": uui}},\
            {"$addFields": \
            {"ttt" :{"$arrayElemAt": [ "$ts_val", 0]},"ts": {"$arrayElemAt": [ "$ts_val", 0]}, "val": {"$arrayElemAt": [ "$ts_val", -1]}}},\
            {"$unwind": "$ts"},\
            {"$addFields": \
            {"time": {"$add":["$ts", "$Date"]}, "index": {"$indexOfArray": ["$ttt", "$ts"]}}},\
            {"$addFields":{"value": {"$arrayElemAt": ["$val", "$index"]}}},\
            {"$match": { "time": {'$gte': time1}}},\
            {"$project": {"uuid": 0, "Date": 0, "ts_val": 0, "ts" : 0, "val": 0, "ttt" : 0}}]
    res = coll.aggregate(pipe)
    res1 = pd.DataFrame(list(coll.aggregate(pipe)))
    print(res1)
    count = 0
    for i in res:
        count += 1
        coll.update_one({"_id": i['_id']}, {"$unset" : {'ts_val.1.'+str(i['index']):1,'ts_val.0.'+str(i['index']):1}})
    coll.update_many({}, {"$pull" : {'ts_val.1': None, 'ts_val.0':None}})
    print('Deleting number of Time and Values: '+ str(count))

