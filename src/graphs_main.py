import sys
import matplotlib as mpl
import matplotlib.pyplot as plt

from project_art import logo1_1, logo_isim

import numpy as np

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains, sum, avg, count

spark = SparkSession.builder.master("local[*]").appName("Capstone Project graphs").getOrCreate()

data_files_folder = "C:/Users/Learner_9ZH3Z128/Documents/PerScholas_Work/Perscholas_Project_Files/CapstoneProject/data_files/"
data_file_loan_app = "cdw_sapp_loan_application.json"
data_file_credit_app = "cdw_sapp_credit.json"
data_file_branch = "cdw_sapp_branch.json"

# keep this part commented for a while 1to get solutionsto questions
# you can uncomment afterwards
#

df_loan_app = spark.read.json(data_files_folder + data_file_loan_app)
# print(type(df_loan_app))


df_credit_app = spark.read.json(data_files_folder + data_file_credit_app)
# df_credit_app.filter(col("MONTH")=="5")


def visual_1(df_loan_app):
    #print(df_loan_app.filter(col("Self_Employed")=="Yes").count())
    #print(df_loan_app.filter(col("Self_Employed")=="Yes").filter(col("Application_Status")=="Y").count())
    #print(df_loan_app.count())

    d1 = df_loan_app.filter(col("Self_Employed")=="Yes").filter(col("Application_Status")=="N").count()
    d2 = df_loan_app.filter(col("Self_Employed")=="Yes").filter(col("Application_Status")=="Y").count()
    
    #x_val = ["self_emp","a_self_emp","corp_emp","a_corp_emp","total"]
    #y_val = [y1, y2, y3, y4, yt]
    # bu sirf hata df_loan_app.plot(kind="box")
    y = np.array([d1,d2])
    plt.pie(y)
    plt.title("Graph for employment types")
    labels = [f"App by self {round(d1/(d1+d2),2)}",f"App by oth {round(d2/(d1+d2),2)}"]
    plt.legend(labels, loc="center right")
    plt.show()
    return

def visual_2(df_loan_app):
    d1 = df_loan_app.filter(col("Married")=="Yes").filter(col("Gender")=="Male").filter(col("Application_Status")=="N").count()
    d2 = df_loan_app.filter(col("Married")=="Yes").filter(col("Gender")=="Male").filter(col("Application_Status")=="Y").count()
    #print("married,app status n, male",d1)
    #print("married,app status y, male",d2)
    #print(df_loan_app.filter(col("Married")=="Yes").filter(col("Gender")=="Male").count())

    y = np.array([d1,d2])
    labels = [f"M Married N/Acc {round(d1/(d1+d2),2)}",f"M Married   Acc {round(d2/(d1+d2),2)}"]

    plt.pie(y)
    plt.title("Graph for Credit Approval Married/Unmarried")
    plt.legend(labels,loc="center right")

    plt.show()
    return

def visual_3(df_credit_app):
    cr = {}
    for i in range(1,13):
        cr[i]=df_credit_app.filter(df_credit_app.MONTH == i).count()
    keys = list(cr.keys())
    values = list(cr.values())
    sorted_value_index = np.argsort(values)

    sorted_cr = {keys[i]:values[i] for i in sorted_value_index}
    list_x = []
    list_y = []
    for _ in range(3):
        t = sorted_cr.popitem()
        list_x.append(t[0])
        list_y.append(t[1])
    print(list_x, list_y)

    fig = plt.figure(figsize = (10,5))
    plt.bar(list_x, list_y, color='green',width = 0.6)
    plt.xlabel("Number of Month")
    plt.ylabel("Number of Transactions")
    plt.title("Highest number of transactions graph")
    plt.show()
    return

def visual_4(df_credit):
    print("Work in progress")

#visual_1(df_loan_app)
#visual_2(df_loan_app)
#visual_3(df_credit_app)

def page_zeor():
    pass

def page_one():
    print(logo1_1)
    print("\t\tYou have following options (x for exit): ")
    print("\t\t-) Graph of employment types:           1")
    print("\t\t-) Graph of married unmarried approval: 2")
    print("\t\t-) Graph of highest transaction number: 3")
    print("\t\t-) Graph of highest transaction branch: 4")
    functions = {'1':visual_1, '2':visual_2, '3':visual_3, '4':visual_4}
    cancel = 2
    while cancel > 0:
        selection = input("Select a choice: ")
        if selection not in "1234":
            if selection =='x' or selection == 'X':
                sys.exit()
            cancel -= 1
            print(f"You can try {cancel} more times")
            selection = input("Please enter a valid value (1,2,3,4):")
        elif selection in '12':
            functions[selection](df_loan_app)
        elif selection in '34':
            functions[selection](df_credit_app)
        else:
            print("Not implemented yet")
    print('\n\n',logo_isim,'\n\n\n')


page_one()

spark.stop() # below here is to keep the evolution of the functions






#df = pd.DataFrame({'Days':list_x, 'Sales':list_y})
#ax = df.plot.bar(x='Days',y='Sales',rot=0)
#df.plot()


#els = list(sorted_cr.items())[11:8:-1]
#print(els)

#plot_this=[]
#for i in list(sorted_cr)[11:8:-1]:
#    plot_this.append(sorted_cr[i])

'''
df = pd.DataFrame(sorted_cr.keys(), sorted_cr.values())
ax = df.plot.bar(x=sorted_cr.keys(),y=sorted_cr.values(),rot=0)
'''
#ax = df_credit.plot.bar(keys, values, rot=0)


