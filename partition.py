def hash(key,no_of_workers):
    return key%no_of_workers
def partition_fn(fname,path,no_of_workers):
    path_dict={}
    i=0
    while(i<no_of_workers):
        path_dict[i]="Worker"+str(i+1)+"_data/"+fname+"_partition"
        i+=1
    f_map=open(path,"r")
    for line in f_map:
        H_val=hash(ord(line[0]),no_of_workers)
        f=open(path_dict[H_val],"a")
        f.write(line)
        f.close()