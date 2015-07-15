#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
待匹配的数据 (tofill)及匹配数据池(pool)，存储于csv文件中，两个csv都要包含主键列，
及数据列。脚本将对tofill的每一条数据字段，在pool文件的数据列中做匹配，如果pool
数据列包含tofile列数据，则将各自主键列建立一条关系记录，存储在结果文件csv中


以上提到的csv文件，格式要求：
    使用逗号分隔，字段可以使用双引号括起。
    不支持字段内换行
    必须包含主键列，主键列不得重复
    主键列在数据列后


---------------------------------

示例：


tofill.csv
----
id,data
201,AAAA
202,BBBB
203,CCCC
204,DDDD
205,EEEE

pool.csv
----
id,data
90001,AAAA/xyz
20002,BBBBabc
20003,12BBBB
20004,EEwowEE
20005,AABBCC


匹配结果将如下
output.csv
----
tofill_pk,tofill_data,pool_pk,pool_data
id,data,id,data
201,AAAA,90001,AAAA/xyz
202,BBBB,20003,12BBBB
---------------------------------



"""


pool_path='pool.csv'
pool_pk_index=0
pool_data_index=1

tofill_path='tofill.csv'
tofill_pk_index=0
tofill_data_index=1

# 每处理指定次数，报告一次处理行数
tick_every_lines=20


import sys
import csv



pool={}
tofill={}
match=[]

reader=csv.reader(file(pool_path,'rb'))

for line in reader:
    #print line[0],line[1]
    #if len(pool) > 100:break
    if len(line)>=pool_data_index:
        pool[line[pool_pk_index]]=line[pool_data_index]

print 'pool file loaded, %s lines'%(len(pool))


reader=csv.reader(file(tofill_path,'rb'))
for line in reader:
    if len(tofill) > 1000:break
    if len(line)>=pool_data_index:
        tofill[line[pool_pk_index]]=line[pool_data_index]

print 'tofill file loaded, %s lines'%(len(tofill))


print 'matching start ...'
src=''
ok=0
tick=0
for f in tofill:
    #print "## %s"%(tofill[f])
    ok=0
    src=''
    tick+=1
    if tick % tick_every_lines == 0:
        print 'running %.6d....'%(tick)
    for p in pool:
        if pool[p].find(tofill[f]) != -1:
            match.append([f,tofill[f],p,pool[p]])
            ok=1
            src=pool[p]
            break
    #    print "    %s .... %s "%(pool[p],ok)
    if ok:
        print "toofill[%s]:%s  found in pool[%s]:%s ...... %s"%(f,tofill[f],p,pool[p],ok)
    else:
    #    print "toofill[%s] NIL"
        pass


#print match

writer=csv.writer(file('output.csv','wb'))
writer.writerow(['tofill_pk','tofill_data','pool_pk','pool_data'])
for line in match:
    writer.writerow(line)

print "matched records written to file output.csv."

