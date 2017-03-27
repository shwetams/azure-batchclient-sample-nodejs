import csv
import json
from azure.storage.blob import BlockBlobService
import datetime
import os
import argparse

#### TODO: Get the configuration

storage_acc_name = 'gecqrhbase'
storage_acc_key = '2V/2YE0CwGYsb9YdqCKSBdNyZel0BFo8ommVZQiocfCcDw/iqotPAPx93N0wLM8TZZ2sqQZJjyT4+fDXK25Bng=='
folder_prefix = "cqr-cps-"
block_blob_service = BlockBlobService(account_name=storage_acc_name,account_key=storage_acc_key)
####

def getfilename(name):

    names = str(name).split('/')
    return names[len(names)-1]



def processcsvfile(fname,seperator,outdir,outfname,tblname):
    block_blob_service.get_blob_to_path(container_name='metafiles',blob_name='headers.json',file_path='headers.json')
    
    with open('headers.json') as headervals:
        jsonkeys = json.load(headervals)
    header = jsonkeys[tblname.lower()]
        
    with open(fname) as inpfile:
        
        allrows = csv.reader(inpfile,delimiter=seperator)
        print("loaded file " + fname)
        all_vals = []
        for rows in allrows:
            i = 0
            line = "{"
            
            for r in rows:
                if i == 0:
                    #line = line + '"' + header[i] + '":"' + r.replace(chr(10),'').replace(chr(13),'') + '"'
                    line = line + '"' + header[i] + '":"' + r.decode('utf-8','ignore') + '"'
                else:
                    #line = line + "," + '"' + header[i] + '":"' + r.replace(chr(10),'').replace(chr(13),'') + '"'
                    line = line + "," + '"' + header[i] + '":"' + r.decode('utf-8','ignore') + '"'
                i = i + 1

            line = line + "}"            
            #print("line : " + line)

            all_vals.append(json.loads(json.dumps(line)))
            line = ""
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        json_fpath = outdir + "/" + outfname+'.json'
        o = open(outdir + "/" + outfname+'.json',mode='w')
        json.dump(all_vals,o)         
        o.close()
        return json_fpath
        
        

if __name__ == "__main__":
    ### TODO ### Replace day and month variable with calculating using datetime

    parser = argparse.ArgumentParser(description="Processes and stores data into hbase")
    parser.add_argument("--year",dest="year")
    parser.add_argument("--month",dest="month")
    parser.add_argument("--day",dest="day")
    parser.add_argument("--memberObj",dest="memberobj")
    parser.add_argument("--hour",dest="hour")
    args = parser.parse_args()

    #memberObj = json.loads(args.memberobj)
    
    year = args.year
    day = args.day
    month = args.month
    if args.hour is not None:
        hour = args.hour
    else:
        hour = None

    
    print("member details:" + args.memberobj)
    print("year : " + year)
    print("month : " + month)
    print("hour : " + hour)
    print("day : " +day)
    #year = "2016"
    #day = "14"
    #month = "11"
    #year = str(datetime.datetime.today().year)
    #day =  str(datetime.datetime.today().day)
    #month = str(datetime.datetime.today().month)
    #with open("memberslist.json") as member_file:
    #    members_list = json.load(member_file)
    #member_file.close()

    members_list = json.loads(args.memberobj)
    for member in members_list:
        memberid = member["id"]
        storage_acc_name = member["storage_acc_name"]
        storage_acc_key = member["storage_acc_key"]
        root_path = member["root_path"]
        folders = member["folders"]
        
        print("found member id " + memberid)
        #generator = block_blob_service.list_blobs( folder_prefix + memberid)
        i = 0        
        for folder in folders:
            #p = root_path + "/" +  folder + "/" + str(datetime.datetime.today().year) + "/" + str(datetime.datetime.today().month) + "/" + day + "/"
            if hour is not None:
                p = root_path + "/" +  folder + "/" + year + "/" + month + "/" + day + "/"
            else:
                p = root_path + "/" +  folder + "/" + year + "/" + month + "/" + day + "/" + hour + "/"
            print("pattern : " + p)
            generator = block_blob_service.list_blobs(container_name= folder_prefix + memberid, prefix= p  )
            
            print("before generator" )
            for blob in generator:
                print("found blob : " + blob.name)
                blob_name = getfilename(blob.name)

                downloadedblob = "downloaded" + blob_name 
                block_blob_service.get_blob_to_path(container_name=folder_prefix + memberid,blob_name=blob.name, file_path=downloadedblob, open_mode='w')                
                json_outpath = processcsvfile(fname=downloadedblob,seperator="|",outfname=blob_name,outdir='jsonfiles/'+ folder_prefix + memberid + "/" + p,tblname=folder)
                i = i + 1
                #print(blob.name)
                #if i == 10:
                print("uploading blob" + json_outpath)
                block_blob_service.create_blob_from_path(container_name=folder_prefix + memberid,blob_name=str(p+ 'json/' +blob_name+".json"),file_path=json_outpath) 
                #break
                
                #i = i + 1
                
            #break
    