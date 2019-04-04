

'''

for each_dir in current_dir:
    
    for file in that dir

'''
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

def main():
    # subfolders = [f.path for f in os.scandir(folder) if f.is_dir()]
    # print(subfolders)
    current_dir = os.getcwd()
    start_pos = 5
    end_pos = 35
    output_file = os.path.join(current_dir,"bigTable.csv")


    data = [os.path.join(current_dir, item) for item in os.listdir(current_dir)
        if os.path.isdir(os.path.join(current_dir, item))]
    
    dir_list = filter(remove_hidden,data)

    for sub_dir in dir_list:
        print("driver_post_process.py {} {} {} {}".format(sub_dir,start_pos,end_pos,output_file))
        driver_post_processing.process(sub_dir, start_pos, end_pos, output_file)


if __name__ == "__main__":
    main()
