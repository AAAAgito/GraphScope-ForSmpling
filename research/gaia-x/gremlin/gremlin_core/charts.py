from os import times
from pyecharts import options as opts
from pyecharts.charts import Bar3D
from pyecharts.faker import Faker
import math


fs = open('C:\\Users\\10514\\Desktop\\data.txt',mode= 'r')

line = fs.readline()

exp =0
times = 0
title =[]
config =0
x=[]
y=[]
glo_dic = {}

while line:
    line.strip()
    if line == "": continue
    if line == "Experiment 3":
        exp = 3
    elif line == "Experiment 2":
        exp = 2
    elif line == "Experiment 1":
        exp = 1
    elif line.count("area"):
        config=line
    elif line.count("sample"):
        title.append(line)
        times+=1
        x.append({})
    

    elif line.count("==")!=0:
        data1 = line.split(": ")
        x[times-1][data1[0]] = math.log(int(data1[1]),10)+1
    line = fs.readline()

for i in x:
    for j in i.keys() :
        glo_dic[j] = 0

pattern_list = []
for i in glo_dic.keys():
    pattern_list.append(i)
pattern_list.sort()
data = [(i, j, x[i].get(pattern_list[j],0)) for i in range(times) for j in range(len(pattern_list))]
c = (
    Bar3D()
    .add(
        "",
        [[d[1], d[0], d[2]] for d in data],
        xaxis3d_opts=opts.Axis3DOpts(pattern_list, type_="category", name=None, name_gap=40),
        yaxis3d_opts=opts.Axis3DOpts(title, type_="category", interval=0, name=None, name_gap=60),
        zaxis3d_opts=opts.Axis3DOpts(type_="value"),
    )
    .set_global_opts(
        visualmap_opts=opts.VisualMapOpts(max_=6),
        title_opts=opts.TitleOpts(title=config[:9]),
    )
    .render("Experiment_1.html")
)

fs = open('C:\\Users\\10514\\Desktop\\data3.txt',mode= 'r')

line = fs.readline()

exp =0
times = 0
title =[]
config =0
x=[]
y=[]
glo_dic = {}

while line:
    line.strip()
    if line == "": continue
    if line == "Experiment 3":
        exp = 3
    elif line == "Experiment 2":
        exp = 2
    elif line == "Experiment 1":
        exp = 1
    elif line.count("area"):
        part = line.split("min")
        config=part[0]
    elif line.count("sample"):
        title.append(config+" "+line[10:])
        times+=1
        x.append({})
    

    elif line.count("==")!=0:
        data1 = line.split(": ")
        x[times-1][data1[0]] = math.log(int(data1[1]),10)+1
    line = fs.readline()

for i in x:
    for j in i.keys() :
        glo_dic[j] = 0

pattern_list = []
for i in glo_dic.keys():
    pattern_list.append(i)
pattern_list.sort()
data = [(i, j, x[i].get(pattern_list[j],0)) for i in range(times) for j in range(len(pattern_list))]
c = (
    Bar3D()
    .add(
        "",
        [[d[1], d[0], d[2]] for d in data],
        xaxis3d_opts=opts.Axis3DOpts(pattern_list, type_="category", name=None, name_gap=40),
        yaxis3d_opts=opts.Axis3DOpts(title, type_="category", interval=0, name=None, name_gap=60),
        zaxis3d_opts=opts.Axis3DOpts(type_="value"),
    )
    .set_global_opts(
        visualmap_opts=opts.VisualMapOpts(max_=6),
        title_opts=opts.TitleOpts(title="Experiment3"),
    )
    .render("Experiment_3.html")
)



fs = open('C:\\Users\\10514\\Desktop\\data2.txt',mode= 'r')

line = fs.readline()

exp =0
times = 0
title =[]
config =0
x=[]
y=[]
glo_dic = {}

while line:
    line.strip()
    if line == "": continue
    if line == "Experiment 3":
        exp = 3
    elif line == "Experiment 2":
        exp = 2
    elif line == "Experiment 1":
        exp = 1
    elif line.count("area"):
        part = line.split("min")
        config=part[0]
    elif line.count("sample"):
        title.append(config+" "+line[10:])
        times+=1
        x.append({})
    

    elif line.count("==")!=0:
        data1 = line.split(": ")
        x[times-1][data1[0]] = math.log(int(data1[1]),10)+1
    line = fs.readline()

for i in x:
    for j in i.keys() :
        glo_dic[j] = 0

pattern_list = []
for i in glo_dic.keys():
    pattern_list.append(i)
pattern_list.sort()
data = [(i, j, x[i].get(pattern_list[j],0)) for i in range(times) for j in range(len(pattern_list))]
c = (
    Bar3D()
    .add(
        "",
        [[d[1], d[0], d[2]] for d in data],
        xaxis3d_opts=opts.Axis3DOpts(pattern_list, type_="category", name=None, name_gap=40),
        yaxis3d_opts=opts.Axis3DOpts(title, type_="category", interval=0, name=None, name_gap=60),
        zaxis3d_opts=opts.Axis3DOpts(type_="value"),
    )
    .set_global_opts(
        visualmap_opts=opts.VisualMapOpts(max_=6),
        title_opts=opts.TitleOpts(title="Experiment3"),
    )
    .render("Experiment_2.html")
)