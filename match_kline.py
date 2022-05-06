import json
import happybase
import numpy as np
import matplotlib.pyplot as plt
from sklearn import preprocessing
from dtw import *
scaler = preprocessing.MinMaxScaler()


hbase = happybase.Connection(host='192.168.137.128')
stock_table = hbase.table('stock')

count = 0
xlist = []
ylist = []
codelist = []
for key,data in stock_table.scan(columns=['info:kline']):
    count = count + 1
    kline = str(data[b'info:kline'], 'utf-8').split(';')
    x=[]
    y=[]
    j = 0
    for i in kline:
        j = j + 1
        if j < 60:
            continue
        if j > 120:
            break
        sp = i.split(',')
        x.append(int(j))
        y.append((float(sp[1])+float(sp[2]))/2)
    if len(y) < 2:
        continue
    codelist.append(key)
    xlist.append(np.array(x))
    y = np.array(y).reshape(-1, 1)
    y_norm = scaler.fit_transform(y)
    ylist.append(y_norm)
    if count > 1:
        break

print('start fit')
# # print(ylist)
template = ylist[0]
min = 999999999999999
emin = 999999999999999
eindex = 0
index = 0
for i in range(1,len(ylist)):

    query = ylist[i]
    alignment = dtw(query, template, keep_internals=True)

    if min > alignment.distance:
        index = i
        min = alignment.distance
    if len(query) == len(template) and emin > np.linalg.norm(query - template):
        eindex = i
        emin = np.linalg.norm(query - template)
    # print(alignment.distance)
    # alignment.plot(type="threeway")
#
print(min,index,codelist[index])
print(emin,eindex,codelist[eindex])
plt.plot(xlist[0], ylist[0])
plt.plot(xlist[index], ylist[index], linestyle="--")
plt.plot(xlist[eindex], ylist[eindex], linestyle=":")
plt.show()