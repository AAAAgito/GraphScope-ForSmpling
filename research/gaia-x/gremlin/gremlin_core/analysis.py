import csv
fs = open('data.txt',mode= 'r')
csvfile = open("data.csv",'a',newline='')
writer = csv.writer(csvfile)
line = fs.readline()
label = ()
store = {}
count =0
local_store = []
while line:
    if line.startswith("area") :
        if label != ():
            store[label] = local_store.copy()
        area_class = ((int(line[6:8]), int(line[21])))
        writer.writerow([int(line[6:8]), int(line[21])])
        label = area_class
        store[label] = []
        count = 0
        local_store.clear()
    elif line.startswith("end") :
        store[label] = local_store.copy()
    else:
        data =line.strip().split("|")
        res_list =[]
        for i in data:
            res = i.strip()
            if res =="":
                continue
            res_list.append(float(res))
        if res_list != []:
            local_store.append(res_list)
            writer.writerow(res_list)
        count+=1
    line=fs.readline()

# x:rate, y:acc, line: area
# def gen_data_area(min_pattern):
#     line=[10,20,30,40,50,60,70]
#     x=[5,10,20,30,40,50,60]
#     avg_area = []
#     stddev_area = []
#     for i in line:
#         dataset = store[(i,min_pattern)]
#         local_avg = {0.0,0.0,0.0,0.0,0.0,0.0,0.0}
#         for j in range(len(dataset)):
#             for k in range(len(x)):
#                 local_avg[k] += dataset[j][k]
#         for l in range(len(local_avg)):
#             local_avg[l] = local_avg/ float(len(local_avg))
#         avg_area.append(local_avg.copy())

#     for i in avg_area:
#         dataset = store[(i,min_pattern)]
#         local_var = {0.0,0.0,0.0,0.0,0.0,0.0,0.0}
#         for j in range(len(dataset)):
#             for k in range(len(x)):
#                 local_var[k] += (dataset[j][k]-i)**2
#         for l in range(len(local_var)):
            

#     return


for i in store.items():
    print(i)