from collections import defaultdict
import os
import sys
'''
node to container mapping
cart-db44db4b9-q54pf kb-w11
catalogue-f4f7886b-4b82k kb-w13
catalogue-f4f7886b-qnwbf kb-w41
catalogue-f4f7886b-t5drm kb-w12
catalogue-f4f7886b-txqdq kb-w31
dispatch-566f964cbd-xrsjf kb-w13
mongodb-6b45dd6dcc-rng5p kb-w14
mysql-7dd8588fb5-qz6fv kb-w21
payment-689b48b7d9-6785z kb-w22
rabbitmq-b4c48cb79-f9r9v kb-w42
ratings-6bbf8c588d-7qkhh kb-w24
ratings-6bbf8c588d-f2m86 kb-w34
redis-7fbd75b76d-krskv kb-w31
shipping-bd5b4b46f-cgfjz kb-w34
stream-2sglh kb-w24
stream-89b2s kb-w24
stream-8n7nz kb-w24
stream-h8m9q kb-w24
stream-t2hvj kb-w44
stream-txkrx kb-w24
user-69d787d68b-l8rmg kb-w33
web-6f7c94568-h49r4 kb-w41

node_map  = dict()
node_map['cart'] = ['kb-w11','kb-w12','kb-w13','kb-w14']
node_map['catalogue'] = ['kb-w11','kb-w12','kb-w13','kb-w14']
node_map['cart'] = ['kb-w11','kb-w12','kb-w13','kb-w14']
node_map['cart'] = ['kb-w11','kb-w12','kb-w13','kb-w14']
node_map['cart'] = ['kb-w11','kb-w12','kb-w13','kb-w14']
node_map['cart'] = ['kb-w11','kb-w12','kb-w13','kb-w14']


'''

def read_container_host_mapping(current_dir, mapFile="container_node_mapping.csv"):
    
    mapping = defaultdict(list)
    mapFile = os.path.abspath(os.path.join(current_dir, mapFile))

    with open (mapFile,'r') as f:
        for line in f:
            container, node = line.strip('\n').split(' ')
            container = container.split('-')[0] #very first section of the pod name 
            mapping[container].append(node)
    

    return mapping 

        



# Import the os module, for the os.walk function
import os
import driver_post_processing 


def remove_hidden(folderName):
    drop_list = set()
    drop_list.add(".git")
    for item in drop_list:
        if item in folderName :
            return False
    return True


# Set the directory you want to start from

def main(current_dir=""):
    debug = True 
    if not current_dir:
        current_dir = os.getcwd()
    else:
        current_dir = os.path.abspath(current_dir)
    
    start_pos = 5
    end_pos = 35
    output_file = "bigtable.csv"
    mapFile = "container_node_mapping.csv"

    mapping = read_container_host_mapping(current_dir, mapFile)
    if debug:
        print("[debug] mapping: {}".format(mapping))



    data = [os.path.join(current_dir, item) for item in os.listdir(current_dir)
        if os.path.isdir(os.path.join(current_dir, item))]
    
    dir_list = filter(remove_hidden,data)

    for sub_dir in dir_list:
        if debug:
            print("driver_post_process.py {} {} {} {}".format(sub_dir,start_pos,end_pos,output_file))
        #mapping = read_container_host_mapping(sub_dir, mapFile)  #TODO: mapFile shall be local for each directory 
        try:
            result = driver_post_processing.process(sub_dir, start_pos, end_pos, mapping)
            print(result)
        except:
            continue


if __name__ == "__main__":
    main(sys.argv[1])
