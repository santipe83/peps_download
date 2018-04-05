#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
import json
import time
import os, os.path, optparse,sys
from datetime import date
import subprocess
import requests
import zipfile

###########################################################################
class OptionParser (optparse.OptionParser):
 
    def check_required (self, opt):
      option = self.get_option(opt)
 
      # Assumes the option's 'default' is set to None!
      if getattr(self.values, option.dest) is None:
          self.error("%s option not supplied" % option)
 
###########################################################################


def get_product_path(productID, parentFolder):
    """ Function to get the file path (.zip) from a given product identifier :
    >>> parse_product("S1A_IW_OCN__2SDV_20180114T174658_20180114T174723_020154_022602_E24D","/work/OT/peps/products/")
    /work/OT/peps/products/2018/01/14/S1A/S1A_IW_OCN__2SDV_20180114T174658_20180114T174723_020154_022602_E24D.zip
    """
    splitList = productID.split("_")
    splitList = [i for i in splitList if i !=""]
    product = str(splitList[0])
    year = str(splitList[4][0:4])
    month = str(splitList[4][4:6])
    day = str(splitList[4][6:8])
    result = str(parentFolder + year + "/" + month + "/"  + day + "/" + product+ "/" + productID + ".zip" )
    return result

#==================
#parse command line
#==================
if len(sys.argv) == 1:
    prog = os.path.basename(sys.argv[0])
    print '      '+sys.argv[0]+' [options]'
    print "     Aide : ", prog, " --help"
    print "        ou : ", prog, " -h"
    print "example 1 : python %s -l 'Toulouse' -a peps.txt -d 2016-12-06 -f 2017-02-01 -c S2ST" %sys.argv[0]
    print "example 2 : python %s --lon 1 --lat 44 -a peps.txt -d 2015-11-01 -f 2015-12-01 -c S2"%sys.argv[0]
    print "example 3 : python %s --lonmin 1 --lonmax 2 --latmin 43 --latmax 44 -a peps.txt -d 2015-11-01 -f 2015-12-01 -c S2"%sys.argv[0]
    print "example 4 : python %s -l 'Toulouse' -a peps.txt -c SpotWorldHeritage -p SPOT4 -d 2005-11-01 -f 2006-12-01"%sys.argv[0]
    print "example 5 : python %s -c S1 -p GRD -l 'Toulouse' -a peps.txt -d 2015-11-01 -f 2015-12-01"%sys.argv[0]
 
    sys.exit(-1)
else :
    usage = "usage: %prog [options] "
    parser = OptionParser(usage=usage)
  
    parser.add_option("-l","--location", dest="location", action="store", type="string", \
            help="town name (pick one which is not too frequent to avoid confusions)",default=None)		
    parser.add_option("-a","--auth", dest="auth", action="store", type="string", \
            help="Peps account and password file")
    parser.add_option("-w","--write_dir", dest="write_dir", action="store",type="string",  \
            help="Path where the products should be downloaded",default='.')
    parser.add_option("-c","--collection", dest="collection", action="store", type="choice",  \
            help="Collection within theia collections",choices=['S1','S2','S2ST','S3'],default='S2')
    parser.add_option("-p","--product_type", dest="product_type", action="store", type="string", \
            help="GRD, SLC, OCN (for S1) | S2MSI1C (for S2)",default="")
    parser.add_option("-m","--sensor_mode", dest="sensor_mode", action="store", type="string", \
            help="EW, IW , SM, WV (for S1) | INS-NOBS, INS-RAW (for S2)",default="")
    parser.add_option("-n","--no_download", dest="no_download", action="store_true",  \
            help="Do not download products, just print curl command",default=False)
    parser.add_option("-d", "--start_date", dest="start_date", action="store", type="string", \
            help="start date, fmt('2015-12-22')",default=None)
    parser.add_option("--lat", dest="lat", action="store", type="float", \
            help="latitude in decimal degrees",default=None)
    parser.add_option("--lon", dest="lon", action="store", type="float", \
            help="longitude in decimal degrees",default=None)
    parser.add_option("--latmin", dest="latmin", action="store", type="float", \
            help="min latitude in decimal degrees",default=None)
    parser.add_option("--latmax", dest="latmax", action="store", type="float", \
            help="max latitude in decimal degrees",default=None)
    parser.add_option("--lonmin", dest="lonmin", action="store", type="float", \
            help="min longitude in decimal degrees",default=None)
    parser.add_option("--lonmax", dest="lonmax", action="store", type="float", \
            help="max longitude in decimal degrees",default=None)
    parser.add_option("-o","--orbit", dest="orbit", action="store", type="int", \
            help="Orbit Path number",default=None)
    parser.add_option("-f","--end_date", dest="end_date", action="store", type="string", \
            help="end date, fmt('2015-12-23')",default=None)
    parser.add_option("--json", dest="search_json_file", action="store", type="string", \
            help="Output search JSON filename", default=None)

    parser.add_option("--amalthee", dest="amalthee", action="store_true", \
            help="Sets the Amalthee API", default=False)
    (options, args) = parser.parse_args()

if options.search_json_file==None or options.search_json_file=="":
    options.search_json_file='search.json'

if options.location==None:    
    if options.lat==None or options.lon==None:
        if options.latmin==None or options.lonmin==None or options.latmax==None or options.lonmax==None:
            print "provide at least a point or rectangle"
            sys.exit(-1)
        else:
            geom='rectangle'
    else:
        if options.latmin==None and options.lonmin==None and options.latmax==None and options.lonmax==None:
            geom='point'
        else:
            print "please choose between point and rectangle, but not both"
            sys.exit(-1)
            
else :
    if options.latmin==None and options.lonmin==None and options.latmax==None and options.lonmax==None and options.lat==None or options.lon==None:
        geom='location'
    else :
          print "please choose location and coordinates, but not both"
          sys.exit(-1)

# geometric parameters of catalog request          
if geom=='point':
    query_geom='lat=%f\&lon=%f'%(options.lat,options.lon)
elif geom=='rectangle':
    query_geom='box={lonmin},{latmin},{lonmax},{latmax}'.format(latmin=options.latmin,latmax=options.latmax,lonmin=options.lonmin,lonmax=options.lonmax)
elif geom=='location':
    query_geom="q=%s"%options.location

# date parameters of catalog request    
if options.start_date!=None:    
    start_date=options.start_date
    if options.end_date!=None:
        end_date=options.end_date
    else:
        end_date=date.today().isoformat()

# Amalthee download
if options.amalthee==False :
    print("hello")    



# special case for Sentinel-2

if options.collection=='S2':
    if  options.start_date>= '2016-12-05':
        print "**** products after '2016-12-05' are stored in Tiled products collection"
        print "**** please use option -c S2ST"
        time.sleep(5)
    elif options.end_date>= '2016-12-05':
        print "**** products after '2016-12-05' are stored in Tiled products collection"
        print "**** please use option -c S2ST to get the products after that date"
        print "**** products before that date will be downloaded"
        time.sleep(5)

if options.collection=='S2ST':
    if  options.end_date< '2016-12-05':
        print "**** products before '2016-12-05' are stored in non-tiled products collection"
        print "**** please use option -c S2"
        time.sleep(5)
    elif options.start_date< '2016-12-05':
        print "**** products before '2016-12-05' are stored in non-tiled products collection"
        print "**** please use option -c S2 to get the products before that date"
        print "**** products after that date will be downloaded"
        time.sleep(5)


if options.amalthee==False:
    # ====================
    # read authentification file
    # ====================
    try:
        f=file(options.auth)
        (email,passwd)=f.readline().split(' ')
        if passwd.endswith('\n'):
            passwd=passwd[:-1]
        f.close()
    except :
        print "error with password file"
        sys.exit(-2)


    if os.path.exists(options.search_json_file):
        os.remove(options.search_json_file)



    # search in catalog
    if (options.product_type=="") and (options.sensor_mode=="") :
        #search_catalog='curl -k -o %s https://peps.cnes.fr/resto/api/collections/%s/search.json?%s&startDate=%s\&completionDate=%s\&maxRecords=500'%(options.search_json_file,options.collection,query_geom,start_date,end_date)
        search_catalog='curl -k -o %s https://peps.cnes.fr/resto/api/collections/%s/search.json?%s&startDate=%s&completionDate=%s&maxRecords=500'%(options.search_json_file,options.collection,query_geom,start_date,end_date)
    else :
        search_catalog='curl -k -o %s https://peps.cnes.fr/resto/api/collections/%s/search.json?%s&startDate=%s&completionDate=%s&maxRecords=500&productType=%s&sensorMode=%s'%(options.search_json_file,options.collection,query_geom,start_date,end_date,options.product_type,options.sensor_mode)



    print search_catalog
    os.system(search_catalog)
    time.sleep(5)

    # Filter catalog result
    with open(options.search_json_file) as data_file:
        data = json.load(data_file)

    if 'ErrorCode' in data :
        print data['ErrorMessage']
        sys.exit(-2)

    #Sort data
    download_dict={}
    storage_dict={}
    for i in range(len(data["features"])):
        prod      =data["features"][i]["properties"]["productIdentifier"]
        feature_id=data["features"][i]["id"]
        storage   =data["features"][i]["properties"]["storage"]["mode"]
        platform  =data["features"][i]["properties"]["platform"]
        #recup du numero d'orbite
        orbitN=data["features"][i]["properties"]["orbitNumber"]
        if platform=='S1A':
        #calcul de l'orbite relative pour Sentinel 1A
            relativeOrbit=((orbitN-73)%175)+1
        elif platform=='S1B':
        #calcul de l'orbite relative pour Sentinel 1B
            relativeOrbit=((orbitN-27)%175)+1

        print data["features"][i]["properties"]["productIdentifier"],data["features"][i]["id"],data["features"][i]["properties"]["startDate"],storage

        if options.orbit!=None:
            if platform.startswith('S2'):
                if prod.find("_R%03d"%options.orbit)>0:
                    download_dict[prod]=feature_id
                    storage_dict[prod]=storage
            elif platform.startswith('S1'):
                if relativeOrbit==options.orbit:
                    download_list[prod]=feature_id
                    storage_list[prod]=storage
        else:
            download_dict[prod]=feature_id
            storage_dict[prod]=storage


    #====================
    # Download
    #====================


    if len(download_dict)==0:
        print "No product matches the criteria"
    else:
        for prod in download_dict.keys():
            if options.write_dir==None :
                options.write_dir=os.getcwd()
            file_exists= os.path.exists(("%s/%s.SAFE")%(options.write_dir,prod)) or  os.path.exists(("%s/%s.zip")%(options.write_dir,prod))
            tmticks=time.time()
            tmpfile=("%s/tmp_%s.tmp")%(options.write_dir,tmticks)
            print "\nDownload of product : %s"%prod
            get_product='curl -o %s -k -u %s:%s https://peps.cnes.fr/resto/collections/%s/%s/download/?issuerId=peps'%(tmpfile,email,passwd,options.collection,download_dict[prod])
            print get_product
            if (not(options.no_download) and not(file_exists)):
                if storage_dict[prod]=="tape":
                    #downloading product from tape requires several attemps, waiting for the tape to be read
                    print "\n***product is on tape, we'll have to wait a little"
                    for attempt in range(5):
                        print "\t attempt", attempt+1
                        os.system(get_product)
                        if not os.path.exists(('tmp_%s.tmp')%(tmticks)):
                            time.sleep(45)
                            if attempt==4 :
                                print "*********download timed out**********"
                                sys.exit(-2)
                        else:
                            break

                else :
                    os.system(get_product)
                #check if binary product
                with open(tmpfile) as f_tmp:
                    try:
                        tmp_data=json.load(f_tmp)
                        print "Result is a text file (might come from a wrong password file)"
                        print tmp_data
                        sys.exit(-1)
                    except ValueError:
                        pass
                os.rename("%s"%tmpfile,"%s/%s.zip"%(options.write_dir,prod))
                print "product saved as : %s/%s.zip"%(options.write_dir,prod)
            elif file_exists:
                print "%s already exists"%prod
            elif options.no_download:
                print "no download (-n) option was chosen"
else:
    
    if os.path.exists(options.search_json_file):
        os.remove(options.search_json_file)

    # search in catalog
    if (options.product_type == "") and (options.sensor_mode == ""):
        search_catalog = 'https://peps.cnes.fr/resto/api/collections/%s/search.atom?%s&startDate=%s&completionDate=%s&maxRecords=500' %(options.collection, query_geom, start_date, end_date)
    else:
        search_catalog = 'https://peps.cnes.fr/resto/api/collections/%s/search.atom?%s&startDate=%s&completionDate=%s&maxRecords=500&productType=%s&sensorMode=%s'%(options.collection, query_geom, start_date, end_date, options.product_type,options.sensor_mode)

    print "Search catalog: ", search_catalog
 
    proc = subprocess.Popen([str("./launch_amalthee.py \"" + search_catalog +"\"")], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    print out
    
    # Command_id is defined by the last word of the launchu_amalthee.py output 
    command_id = out.split()[-1]

    # Command_json_url
    command_json_url = str("http://peps-vizo.cnes.fr:8081/wps/outputs/report-"+command_id+".json")
   
    # Initialization of Job_status for the first iteration
    job_status = "RUNNING" 
    while (job_status in ["STALLED","RUNNING"]):
        time.sleep(10)
        os.system(str("./launch_amalthee.py " + command_id))
        #proc = subprocess.Popen([str("./launch_amalthee.py "+command_id)], stdout=subprocess.PIPE, shell=True)
        #(out, err) = proc.communicate()
        resp = requests.get(command_json_url)
        try : 
            data = json.loads(resp.text)
        except ValueError,e:
            print "ERROR : Error in Amalthee processing: Status Json not available... exiting...."
            sys.exit(-1) 
        if ("job_status" in data["USER_INFO"]):
            job_status = data["USER_INFO"]["job_status"] 
        elif ("status" in data["USER_INFO"]):
            job_status = data["USER_INFO"]["status"]
        else: 
            jobs_status = "ERROR"
            print "ERROR : Error in Amalthee processing: Job status not recognized... exiting...."
            sys.exit(-1)
        print job_status
    

    counter_zip_errors = 0 
    print "PEPS Files transfered to /work/OT/peps/product. Ready to Unzip!" 
    if "product_list" in data["USER_INFO"] :
        for i in data["USER_INFO"]["product_list"] :
            zipPath =  get_product_path(i,data["USER_INFO"]["dir_product"])
            print zipPath
            try:
                with zipfile.ZipFile(zipPath,'r') as zip_ref:
                    print "Unzipping : ", zipPath , " ........"
                    zip_ref.extractall(str(options.write_dir))
            except zipfile.BadZipfile:
		    print ">>> ERROR unzipping :", zipPath 
                    counter_zip_errors += 1
                    continue
            except IOError:
		    print ">>> ERROR No such file :", zipPath 
                    counter_zip_errors += 1
                    continue
        print "Zip files completed: ", str(len(data["USER_INFO"]["product_list"]) - counter_zip_errors)
        print "Zip files in error: ", str(counter_zip_errors)
       
    else :
        print "ERROR : the json file format is not valid : the field 'USER_INFO','product_list' is not available."
        sys.error(-1)



