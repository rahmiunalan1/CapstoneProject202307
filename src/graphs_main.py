import sys
import matplotlib as mpl
import matplotlib.pyplot as plt

from project_art import logo1_1, logo_isim

import numpy as np

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, countDistinct

spark = SparkSession.builder.master("local[*]").appName("Capstone Project graphs").getOrCreate()

data_files_folder = "C:/Users/Learner_9ZH3Z128/Documents/PerScholas_Work/Perscholas_Project_Files/CapstoneProject/data_files/"
data_file_loan_app = "cdw_sapp_loan_application.json"
data_file_credit_app = "cdw_sapp_credit.json"
data_file_branch_app = "cdw_sapp_branch.json"
data_file_customer_app = "cdw_sapp_customer.json"

# keep this part commented for a while 1to get solutionsto questions
# you can uncomment afterwards
#

df_loan_app = spark.read.json(data_files_folder + data_file_loan_app)
print(type(df_loan_app))
# print(type(df_loan_app))

df_credit_app = spark.read.json(data_files_folder + data_file_credit_app)
# df_credit_app.filter(col("MONTH")=="5")

df_customer_app = spark.read.json(data_files_folder + data_file_customer_app)
# dfcustomer_app.filter(col("CUST_ZIP")=="48124").show(truncate=False) 

df_branch_app = spark.read.json(data_files_folder + data_file_branch_app)
#df_branch_app.select(df_branch_app.BRANCH_CODE, df_branch_app.BRANCH_ZIP).filter(col("TRANSACTION_TYPE")=="Healthcare")

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
    '''Visual for high rate of transactions'''
    print("Work in progress")
    return

def visual_5(df_credit):
    pass

def visual_6(df_customer_app):
    '''Visual for state with high number of customer'''
    print("Merhaba")
    d1 = df_customer_app.select(df_customer_app.CUST_STATE).distinct().count()#show(truncate=False)
    print(f"\n%%%%%%%%\nd1 is : {d1}\n %%%%%%\n")
    return

def visual_7():
    '''Sum of all transactions for top 10 customers'''
    pass


#visual_1(df_loan_app)
#visual_2(df_loan_app)
#visual_3(df_credit_app)


def page_one():
    print(logo1_1)
    print("\t  You have following options for this page (x for exit): ")
    print("\t  -) Graph of employment types           : 1")
    print("\t  -) Graph of married unmarried approval : 2")
    print("\t  -) Graph of highest transaction number : 3")
    print("\t  -) Graph of highest transaction branch : 4")
    print("\t  -) Graph of highest transaction type   : 5")
    print("\t  -) Graph of highest number of customer : 6")

    functions = {'1':visual_1, '2':visual_2, '3':visual_3, '4':visual_4, '5':visual_5, '6':visual_6}
    cancel = 2
    while cancel > 0:
        selection = input("\nSelect a choice: ")
        if selection not in "123456":
            if selection =='x' or selection == 'X':
                print('\n\n',logo_isim,'\n\n\n')
                sys.exit()
            cancel -= 1
            print(f"You can try {cancel} more times")
            selection = input("Please enter a valid value (1,2,3,4,5,6):")
        elif selection in '12':
            functions[selection](df_loan_app)
        elif selection in '345':
            functions[selection](df_credit_app)
        elif selection in '6':
            functions[selection](df_customer_app)
        else:
            print("Not implemented yet")
    print('\n',logo_isim,'\n\n')

###################################################################
# 333333333333333333333333333333333333333333333333333333333333333 #
###################################################################


def page_zero_a(df_customer_app,df_credit_app):

    print("To display transactions made from a zip code ")
    zipcode = input("Enter zip code: ")
    month = input("Enter month     : ")
    year = input("Enter year       : ")
    print(f"You entered {zipcode} for zipcode")

    #df_customer_app.join(df_credit_app, df_customer_app.SSN == df_credit_app.CUST_SSN, "inner").show(truncate=False)
    t = df_customer_app.join(df_credit_app, df_customer_app.SSN == df_credit_app.CUST_SSN, "inner")
    #t.select(t.FIRST_NAME, t.LAST_NAME, t.CUST_EMAIL, t.BRANCH_CODE,\
    #         t.DAY, t.MONTH, t.YEAR, t.TRANSACTION_ID, t.TRANSACTION_TYPE, t.TRANSACTION_VALUE)\
    #         .filter(col("CUST_ZIP")==zipcode).filter(col("MONTH")==month).filter(col("YEAR")==year).show(truncate=False)
    
    result = t.select(t.FIRST_NAME, t.LAST_NAME, t.CUST_EMAIL, t.BRANCH_CODE,
                  t.DAY, t.MONTH, t.YEAR, t.TRANSACTION_ID, t.TRANSACTION_TYPE, t.TRANSACTION_VALUE)\
          .filter(col("CUST_ZIP") == zipcode).filter(col("MONTH") == month).filter(col("YEAR") == year)\
          .orderBy(col("DAY").desc())

    result.show(truncate=False)
    #df_customer.printSchema()
    #df_customer.filter(col("CUST_STATE")=="NJ").show(truncate=False)
    #df_customer.filter(col("CUST_ZIP")=="48124").show(truncate=False)  
    # #df_credit.select(df_credit.BRANCH_CODE, df_credit.TRANSACTION_VALUE).filter(col("TRANSACTION_TYPE")=="Gas").show(truncate=False) 

def page_zero_b(df_credit_app):

    # print("To display number and total values of transactions select one of")
    # print("Bills, Education, Entertainment, Gas, Grocery, Healthcare, Test")
    # transaction_type = input("Enter transaction type: ")
    # count = df_credit_app.filter(col("TRANSACTION_TYPE")==transaction_type).count()
    # s = df_credit_app.select(sum("TRANSACTION_VALUE"))#.filter(col("TRANSACTION_TYPE")==transaction_type)
    # # df_credit_app.filter(col("TRANSACTION_TYPE")==transaction_type).sum()
    # print(f"Total item for {transaction_type}: {count}; sum value: {s}\n\n")

    print("To display the number and total value of transactions, select one of the following transaction types:")
    print("Bills, Education, Entertainment, Gas, Grocery, Healthcare, Test")
    transaction_type = input("Enter the transaction type: ")

    count = df_credit_app.filter(col("TRANSACTION_TYPE") == transaction_type).count()
    total_sum = df_credit_app.filter(col("TRANSACTION_TYPE") == transaction_type).agg(sum("TRANSACTION_VALUE")).collect()[0][0]

    print(f"Total items for {transaction_type}: {count}; sum value: {total_sum}\n\n")


    return

def page_zero_c():
    run_c = True
    print("To display total number and total values of transactions")
    state = input("Enter state abbreviation")

    return

def display_account_details(ssn):
    ''' Give social security number to check the details
    use customer json file'''
    try:
        df_customer_app.filter(col("SSN")==ssn).show(truncate=False)
    except:
        print(f"There is no account with {ssn} number")
    return

def modify_account_details():
    pass

def monthly_bill(credit_card_no, month):
    #df_customer_app.
    pass


def page_zero():
    print(logo1_1)
    print("\n\t\t Main Menu\n\n")
    print("\t  You have following options for this page : ")
    print("\t  (x for exit; r for restart)")
    #print("\t -) Display Transaction Details : 1")
    #print("\t -) Display Customer Details    : 2")
    functions = {'4': display_account_details}

    while 1:
        print("\t -) Display transactions made by customers living in a zip code      : 1")
        print("\t -) Display number and total values of transactions for a given type : 2")
        print("\t -) Display total number and values of transactions for a given state: 3")

        print("\t -) Display existing account details                                 : 4")
        print("\t -) Modify customer details                                          : 5")
        print("\t -) Generate bill for month                                          : 6")
        print("\t -) Display transactions between dates: 7")
        selection = input("\nSelect a choice: x to exit   ")

        if selection =='x' or selection == 'X':
            print('\n\n',logo_isim,'\n\n\n')
            sys.exit()
        #if selection == 'r' or selection == 'R':
        #    pass #page_zero()
        if selection == '1':
            page_zero_a(df_customer_app,df_credit_app)
        elif selection == '2':
            page_zero_b(df_credit_app)
        elif selection == '3':
            pass
        elif selection == '4':
            ssn = input("Enter SNN number ")
            functions[selection](ssn)
        elif selection == '5':
            pass
        elif selection == '6':
            pass
        elif selection == '7':
            pass
        else:
            print("invalid selection!")

    return 


#page_one()
page_zero()


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


