import json
import happybase
import numpy as np
import matplotlib.pyplot as plt
from sklearn import preprocessing
from dtw import *
scaler = preprocessing.MinMaxScaler()


hbase = happybase.Connection(host='192.168.137.128')
stock_table = hbase.table('stock')
# res = stock_table.row('000001',columns=['info:trend'])
# trend = str(res[b'info:trend'], 'utf-8')
# print(trend.split(';'))

count = 0
xlist = []
ylist = []
for key,data in stock_table.scan(columns=['info:kline']):
    count = count + 1
    trend = str(data[b'info:kline'], 'utf-8').split(';')
    if len(trend) < 10:
        continue
    format = []
    x=[]
    y=[]
    j = 0
    for i in trend:
        j = j + 1
        # if j < 160:
        #     continue
        # if j > 190:
        #     break
        sp = i.split(',')
        format.append([j, sp[2]])
        x.append(int(j))
        y.append(float(sp[2]))
        print(sp[0])
    x = np.array(x)
    xlist.append(np.array(x[0:241]))
    y = np.array(y[0:241]).reshape(-1, 1)
    y_norm = scaler.fit_transform(y)
    ylist.append(y_norm)

    if count > 1:
        break


print('start fit')


# print(ylist)
# template = ylist[0]
# min = 999999999999999
# emin = 999999999999999
# eindex = 0
# index = 0
# for i in range(1,len(ylist)):
#
#     query = ylist[i]
#     alignment = dtw(query, template, keep_internals=True)
#
#     if min > alignment.distance:
#         index = i
#         min = alignment.distance
#     if emin > np.linalg.norm(query - template):
#         eindex = i
#         emin = np.linalg.norm(query - template)
#     # print(alignment.distance)
#     # alignment.plot(type="threeway")
#
# print(min,index)
# print(emin,eindex)
# plt.plot(xlist[0], ylist[0])
# plt.plot(xlist[index], ylist[index], linestyle="--")
# plt.plot(xlist[eindex], ylist[eindex], linestyle=":")
# plt.show()