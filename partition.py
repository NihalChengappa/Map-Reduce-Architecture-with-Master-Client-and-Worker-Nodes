def hash(key,no_of_workers):
    return key%no_of_workers
def partition_fn(fname,path,no_of_workers):
    # print("parttition")
    path_dict={}
    i=0
    while(i<no_of_workers):
        path_dict[i]="/home/pes2ug20cs224/Desktop/BD-Project/YAMR/Worker"+str(i+1)+"_data/"+fname+"_partition"
        # print(i,path_dict[i])
        i+=1
    f_map=open(path,"r")
    for line in f_map:
        H_val=hash(ord(line[0]),no_of_workers)
        f=open(path_dict[H_val],"a")
        print(path_dict[H_val])
        f.write(line)
        f.close()