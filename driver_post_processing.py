import os
import sys
import glob 
import pandas as pd
import numpy as np 
from collections import defaultdict
#global vars
debug = True 
inputFile = dict()
inputFile['distribution'] = "locust_distribution.csv"
inputFile['requests'] = "locust_requests.csv"

#filePattern['vm']="vmfile.csv"
result = dict()








def get_latency(dir_name):
    result['web'] = 0
    result['cart-add'] = 0
    result['cart-cart'] = 0
    result['cart-update'] = 0
    result['catalogue-product'] = 0
    result['catalogue-categories'] = 0
    result['ratings'] = 0
    result['user'] = 0
    file_name = os.path.join(dir_name,inputFile['distribution'])
    if os.path.isfile(file_name):
        df = pd.read_csv(file_name)
    else:
        return 
    
    #print(df)

    #"Name","# requests","50%","66%","75%","80%","90%","95%","98%","99%","100%"


    for index, row in df.iterrows():
        
        #print(row['Name'],int(row['95%']))
        if row['95%']=="N/A":
            pass
        elif index == 0:
            #print("[debug] first row", row['Name'])
            result["web"] = int(row['95%'])
        elif "cart/cart" in row['Name']:
            result["cart-cart"] = int(row['95%']) if int(row['95%']) > result["cart-cart"]  else result["cart-cart"]
        elif "cart/add" in row['Name']:
            result["cart-add"]=int(row['95%']) if int(row['95%']) > result["cart-add"] else result["cart-add"]
        elif "cart/update" in row['Name']:
              result["cart-update"]=int(row['95%']) if int(row['95%']) > result["cart-update"] else result["cart-update"]
        elif "catalogue/categories" in row['Name']:
            result["catalogue-categories"] = int(row['95%']) if int(
                row['95%']) > result["catalogue-categories"] else result["catalogue-categories"]
        elif "catalogue/product" in row['Name']:
            result["catalogue-product"] = int(row['95%']) if int(
                row['95%']) > result["catalogue-product"] else result["catalogue-product"]
        # elif "ratings" in row['Name']:
        #     result["ratings"]=int(row['95%']) if int(row['95%']) > result["ratings"] else result["ratings"]
        elif "user/uniqueid" in row['Name']:
            result["user"]=int(row['95%']) if int(row['95%']) > result["user"] else result["user"]

    return result

'''
TODO:
disabled for the time being. depending on the requirements, may be used.
needs to changed correspoinding to the columns of the inputFile.
'''
def get_avg_response_time(dir_name):
    result['web'] = 0
    result['cart-add'] = 0
    result['cart-cart'] = 0
    result['cart-update'] = 0
    result['catalogue-product'] = 0
    result['catalogue-categories'] = 0
    result['ratings'] = 0
    result['user'] = 0
    file_name = os.path.join(dir_name, inputFile['requests'])
    if os.path.isfile(file_name):
        df = pd.read_csv(file_name)
    else:
        return

    #print(df)

    #"Name","# requests","50%","66%","75%","80%","90%","95%","98%","99%","100%"

    for index, row in df.iterrows():

        #print(row['Name'],int(row['95%']))
        if row['95%'] == "N/A":
            pass
        elif index == 0:
            #print("[debug] first row", row['Name'])
            result["web"] = int(row['95%'])
        elif "cart/cart" in row['Name']:
            result["cart-cart"] = int(row['95%']) if int(row['95%']
                                                         ) > result["cart-cart"] else result["cart-cart"]
        elif "cart/add" in row['Name']:
            result["cart-add"] = int(row['95%']) if int(row['95%']
                                                        ) > result["cart-add"] else result["cart-add"]
        elif "cart/update" in row['Name']:
              result["cart-update"] = int(row['95%']) if int(
                  row['95%']) > result["cart-update"] else result["cart-update"]
        elif "catalogue/categories" in row['Name']:
            result["catalogue-categories"] = int(row['95%']) if int(
                row['95%']) > result["catalogue-categories"] else result["catalogue-categories"]
        elif "catalogue/product" in row['Name']:
            result["catalogue-product"] = int(row['95%']) if int(
                row['95%']) > result["catalogue-product"] else result["catalogue-product"]
        # elif "ratings" in row['Name']:
        #     result["ratings"]=int(row['95%']) if int(row['95%']) > result["ratings"] else result["ratings"]
        elif "user/uniqueid" in row['Name']:
            result["user"] = int(row['95%']) if int(
                row['95%']) > result["user"] else result["user"]

    return result








    #with open(os.path.abspath(file_name), 'r') as f:
    #    for line in f:

    #for index, row in df.iterrows():
    #print(row['c1'], row['c2'])



    

def get_perf_data(dir_name,start_pos,end_pos):
    files = [name for name in glob.glob(dir_name+"/*_perfstat.csv")]

    result = dict()

    for file in files:
        df = pd.read_csv(file)
        df = df.loc[start_pos:end_pos+1]
        df['cpi'] = df['cycle']/df['instructions']
        cpi = (df['cpi'].mean())
        llc = (df['LLC-load-misses'].mean())
        hostname = df.loc[5]['hostname']
        result[hostname] = {'cpi':cpi,'llc':llc,'hostname':hostname}
    
    return result

def get_cpu_vm(dir_name):
    files = [name for name in glob.glob(dir_name+"/*_vmfile.csv")]
    vm_cpu_avg = dict()

    #go over each files
    for file in files:
        with open(os.path.abspath(file), 'r') as f:
            host_name, val = f.readline().split(':')
            val = float(val)
            vm_cpu_avg[host_name] = val
    
    return vm_cpu_avg




def get_cpu_vm_by_node(dir_name):
    #read from kb-w{}{}_vmfile.csv

    files = [name for name in glob.glob(dir_name+"/*_vmfile.csv")]

    #print("[debug]",files)

    vm_cpu_avg = dict()
    vm_cpu_avg['node1'] = 0
    vm_cpu_avg['node2'] = 0
    vm_cpu_avg['node3'] = 0
    vm_cpu_avg['node4'] = 0

    node1 = set(['kb-w11','kb-w12','kb-w13','kb-w14'])
    node2 = set(['kb-w21', 'kb-w22', 'kb-w23', 'kb-w24'])
    node3 = set(['kb-w31', 'kb-w32', 'kb-w33', 'kb-w34'])
    node4 = set(['kb-w41', 'kb-w42', 'kb-w43', 'kb-w44'])

    cntNode1 = 0
    cntNode2 = 0
    cntNode3 = 0
    cntNode4 = 0

    #go over each files
    for file in files:
        with open (os.path.abspath(file),'r') as f:
            host_name, val = f.readline().split(':')
            val = float(val)
            #print("[debug] from file",host_name,val)
            if host_name in node1:
                vm_cpu_avg['node1'] += val
                cntNode1+=1
            elif host_name in node2:
                vm_cpu_avg['node2'] += val
                cntNode2 += 1
            elif host_name in node3:
                vm_cpu_avg['node3'] += val
                cntNode3 += 1
            elif host_name in node4:
                vm_cpu_avg['node4'] += val
                cntNode4 += 1
    #do the average
    #print("[debug] vm util",vm_cpu_avg)
    #print("[debug] count of nodes",cntNode1,cntNode2,cntNode3,cntNode4)

    #print (vm_cpu_avg)
    try:
        vm_cpu_avg['node1'] /= cntNode1*1.0
        vm_cpu_avg['node2'] /= cntNode2*1.0
        vm_cpu_avg['node3'] /= cntNode3*1.0
        vm_cpu_avg['node4'] /= cntNode4*1.0
    
    except:
        pass


    return vm_cpu_avg













def get_cpu_container():
    pass

def get_mem_container():
    pass

def get_net_container():
    pass


def getHorizontalLine():
    l = 80
    result = "-"*l
    return result 


def get_average_vm_utilization(vm_cpu, node_list):

    sumCpu = 0
    count  = 0
    for node in node_list:
        sumCpu += vm_cpu.get(node,0)
        count += 1 if vm_cpu.get(node,0) >0 else 0
    
    if count ==0:
        return 0
    return sumCpu/count


def get_average_perf(perf_data, node_list):

    sumCpi = 0 
    sumLLC = 0
    count = 0

    for node in node_list:
        if node in perf_data.keys():
            sumCpi += perf_data[node]['cpi']
            sumLLC += perf_data[node]['llc']
            count +=1
    
    if count==0:
        return 0
    sumCpi = sumCpi/count
    sumLLC = sumLLC/count
    return [sumCpi,sumLLC]


def get_95th_latency(latency, service_name):
    pass


def process(dir_name,start_pos,end_pos,mapping):


    # actual aggregation
    result = {}
    #result['test_id'] = dir_name


    latency = get_latency(dir_name)
    vm_cpu = get_cpu_vm(dir_name)
    perf_data =  get_perf_data(dir_name,start_pos,end_pos)


    for service_name, node_name in mapping.items():
        if service_name not in result.keys():
            result[service_name] = {}

        result[service_name]['vm_util'] = get_average_vm_utilization(vm_cpu,node_name)
        result[service_name]['perf_cpi'] = get_average_perf(perf_data,node_name)[0] #first column cpi
        result[service_name]['perf_llc'] = get_average_perf(perf_data, node_name)[1] #second column llc
        
        if service_name == "cart":
            #result['cart-cart'] = {}
            #result['cart-add'] = {}
            #result['cart-update'] = {}
            #result['cart-cart']['95th_latency'] = latency['cart-cart']
            #result['cart-add']['95th_latency'] = latency['cart-add']
            #result['cart-update']['95th_latency'] = latency['cart-update']
            result[service_name]['95th_latency'] = latency['cart-add']
        
        elif service_name == "catalogue":
            #result['catalogue-categories'] = {}
            #result['catalogue-product'] = {}
            #result['catalogue-categories']['95th_latency'] = latency['catalogue-categories']
            #result['catalogue-product']['95th_latency'] = latency['catalogue-product']
            result[service_name]['95th_latency'] = latency['catalogue-product']

        # elif service_name not in latency.keys():
        #     pass


        else:
            result[service_name]['95th_latency'] = latency.get(service_name,0)


    test_id = dir_name.split('/')[-1] #last item

    return (test_id, result)

if __name__ == "__main__":
    process(dir_name=sys.argv[1],start_pos=int(sys.argv[2]),end_pos=int(sys.argv[3]),mapping=sys.argv[4])
