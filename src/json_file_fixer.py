import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains

#file_json = open("C:/Users/rahmi/Documents/Perscholas_Project_Files/analiz0.json")
def fix_branch_file(infile_json_name, outfile_json_name):
    outfile_json = open(outfile_json_name, mode="w")
    infile_json = open(infile_json_name)
    while infile_json:
        line = infile_json.readline()

        if line == "":
            break

        l = line.strip('{\n').strip('}').split(',')
        d = {}
        for e in l:
            k = e.split(':',maxsplit=1)
            d[k[0].strip('"')] = k[1].strip('"')
        
        t0 = int(d['BRANCH_CODE'])
        d['BRANCH_CODE'] = t0

        t1 = d['BRANCH_PHONE']
        t1 = f"({t1[:3]}) {t1[3:6]}-{t1[6:]}"
        d['BRANCH_PHONE'] = t1

        t2 = d['BRANCH_ZIP']
        if len(t2) != 5:
            t2 = '0'+t2
        d['BRANCH_ZIP'] = t2
        json.dump(d, outfile_json, separators=(',',':'))
        outfile_json.write('\n')
    infile_json.close()
    outfile_json.close()
    return

#file_dir = "C:/Users/rahmi/Documents/Perscholas_Project_Files/"
#file_json = file_dir + "analiz0.json"
#fix_branch_file(file_json, file_dir + "analiz0_sonuc.json")

infile_dir = "C:/Users/rahmi/Documents/Perscholas_Project_Files/CapstoneProject/raw_data_files/"
infile_name = infile_dir+"cdw_sapp_branch.json"
outfile_dir = "C:/Users/rahmi/Documents/Perscholas_Project_Files/CapstoneProject/data_files/"
outfile_name = outfile_dir+"cdw_sapp_branch.json"
fix_branch_file(infile_name, outfile_name)

def fix_credit_file(infile_json_name, outfile_json_name):
    outfile_json = open(outfile_json_name,'w')
    infile_json = open(infile_json_name)
    while infile_json:
        line = infile_json.readline()
        
        if line == "":
            break

        l = line.strip('{\n').strip('}').split(',')
        #print(l)
        d = {}
        for e in l:
            k = e.split(':',maxsplit=1)
            d[k[0].strip('"')] = k[1].strip('"')
        
        # convert branch_code to integer
        t0 = int(d['BRANCH_CODE'])
        d['BRANCH_CODE'] = t0

        # create date data from YEAR, MOTH DAY
        # and discard them
        fix_month_day_length = lambda x : x if len(x)==2 else '0'+x
        t1 = d['YEAR'] + fix_month_day_length(d['MONTH']) + fix_month_day_length(d['DAY'])
        #print('t1:',t1)
        d['TIMEID'] = t1
        del d['YEAR']
        del d['MONTH']
        del d['DAY']

        # fill the json file with the data
        json.dump(d, outfile_json, separators=(',',':'))
        outfile_json.write('\n')
    #print(d)
    infile_json.close()
    outfile_json.close()
    return

#fix_credit_file(file_dir+"analiz1.json",file_dir+"analiz1_sonuc.json")

infile_dir = "C:/Users/rahmi/Documents/Perscholas_Project_Files/CapstoneProject/raw_data_files/"
infile_name = infile_dir+"cdw_sapp_credit.json"
outfile_dir = "C:/Users/rahmi/Documents/Perscholas_Project_Files/CapstoneProject/data_files/"
outfile_name = outfile_dir+"cdw_sapp_credit.json"
fix_credit_file(infile_name, outfile_name)


def fix_customer_file(infile_json_name, outfile_json_name):
    outfile_json = open(outfile_json_name,'w')
    infile_json = open(infile_json_name)
    while infile_json:
        line = infile_json.readline()
        
        if line == "":
            break

        l = line.strip('{\n').strip('}').split(',')
        #print(l)
        d = {}
        for e in l:
            k = e.split(':',maxsplit=1)
            d[k[0].strip('"')] = k[1].strip('"')
        
        t0 = d['FIRST_NAME'].title()
        d['FIRST_NAME'] = t0
        t1 = d['MIDDLE_NAME'].lower()
        d['MIDDLE_NAME'] = t1
        t2 = d['LAST_NAME'].title()
        d['LAST_NAME'] = t2

        t3 = d['APT_NO'] +  ' ' + d['STREET_NAME'] + ' '
        d['FULL_STREET_ADDRESS'] = t3

        t4 = d['CUST_PHONE']
        if len(t4)==7:
            t4 = f"(555) {t4[:3]}-{t4[3:]}"
        else:
            t4 = f"({t4[:3]}) {t4[3:6]}-{t4[6:]}"
        d['BRANCH_PHONE'] = t4

        del d['APT_NO']
        del d['STREET_NAME']
        del d['CUST_PHONE']

        json.dump(d, outfile_json, separators=(',',':'))
        outfile_json.write('\n')
    infile_json.close()
    outfile_json.close()
    return

#fix_customer_file(file_dir+"analiz2.json",file_dir+"analiz2_sonuc.json")

infile_dir = "C:/Users/rahmi/Documents/Perscholas_Project_Files/CapstoneProject/raw_data_files/"
infile_name = infile_dir+"cdw_sapp_customer.json"
outfile_dir = "C:/Users/rahmi/Documents/Perscholas_Project_Files/CapstoneProject/data_files/"
outfile_name = outfile_dir+"cdw_sapp_customer.json"
fix_customer_file(infile_name, outfile_name)


#files_location = "C:/Users/rahmi/Documents/Perscholas_Project_Files/analiz0_sonuc.json"
#"C:\Users\rahmi\Documents\Perscholas_Project_Files\analiz0_sonuc.json"
#files_type = "json"

#infer_schema = False
#first_row_is_header = False
#delimiter = ","

#spark = SparkSession.builder.appName("PerScholasProject").getOrCreate()
#df = spark.read.format(files_type).option("inferSchema",infer_schema).option("header",first_row_is_header).option("sep",delimiter).load(files_location)

#print(df)