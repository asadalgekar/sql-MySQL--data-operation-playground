import cx_Oracle
import csv

connection = cx_Oracle.connect("avs4713/AkshaySadalgekar1234@acaddbprod.uta.edu:1523/pcse1p.data.uta.edu")


print("Successfully connected to Oracle Database")

with open("Main.csv","r") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter = ',')
    all_values=[]
    for row in csv_reader:
        while("" in row) :
            row.remove("")
  
        all_values.append(row)
    print(all_values)
     	
    """ extracting particular table details from one csv file"""
    # Bank
    bank_start = all_values.index(["BANK"])
    bank_stop = all_values.index(['BANK_BRANCH'])
    bank_details=[]
    for i in all_values[bank_start+2 : bank_stop]:
        bank_details.append(i)

    # Bank Branch
    bank_branch_start = all_values.index(["BANK_BRANCH"])
    bank_branch_stop = all_values.index(['ACCOUNT'])
    bank_branch_details=[]
    for i in all_values[bank_branch_start+2 : bank_branch_stop]:
        bank_branch_details.append(i)

    # Account
    Account_start = all_values.index(["ACCOUNT"])
    Account_stop = all_values.index(['CUSTOMER'])
    Acount_details=[]
    for i in all_values[Account_start+2 : Account_stop]:
        Acount_details.append(i)
        
    # Customer
    Customer_start = all_values.index(["CUSTOMER"])
    Customer_stop = all_values.index(['LOAN'])
    Customer_details=[]
    for i in all_values[Customer_start+2 : Customer_stop]:
        Customer_details.append(i)

    # Loan
    Loan_start = all_values.index(["LOAN"])
    Loan_stop = all_values.index(["ACCOUNT_CUSTOMER"])
    Loan_details=[]
    for i in all_values[Loan_start+2 : Loan_stop]:
        Loan_details.append(i)

    # Account_customer
    Account_customer_start = all_values.index(["ACCOUNT_CUSTOMER"])
    Account_customer_stop = all_values.index(["LOAN_CUSTOMER"])
    Account_customer_details=[]
    for i in all_values[Account_customer_start+2 : Account_customer_stop]:
        Account_customer_details.append(i)

    # Loan_customer
    Loan_customer_start = all_values.index(["LOAN_CUSTOMER"])
    Loan_customer_stop = len(all_values) + 1
    Loan_customer_details=[]
    for i in all_values[Loan_customer_start+2 : Loan_customer_stop]:
        Loan_customer_details.append(i)

    """ Function to insert values into respective tables """
    # bank_add function
    def bank_add(bank_details):
        sql = ('insert into bank(code, name, addr) ''values(:code,:name,:addr)')
        with connection.cursor() as cursor:
            cursor.executemany(sql, bank_details)
            connection.commit()
        print("Bank Details added in Bank Table")
        print()

# bank_branch_add function
    def bank_branch_add(bank_branch_details):
        sql1 = ('insert into bank_branch(bcode,  branchno, addr) ''values(:bcode,:branchno,:addr)')
        with connection.cursor() as cursor:
            cursor.executemany(sql1, bank_branch_details)
            connection.commit()
        print("Bank Branch Details added in Bank Branch Table")
        print()

# account_add function
    def account_add(account_details):
        sql2 = ('insert into account(acctno,  balance, type, bcode, bno) ''values(:acctno,:balance,:type, :bcode, :bno)')
        with connection.cursor() as cursor:
            cursor.executemany(sql2, account_details)
            connection.commit()
        print("Account Details added in Account Table")
        print()

# account_add function
    def customer_add(customer_details):
        sql3 = ('insert into customer(ssn,  name, phone, addr) ''values(:ssn,  :name, :phone, :addr)')
        with connection.cursor() as cursor:
            cursor.executemany(sql3, customer_details)
            connection.commit()
        print("Customer Details added in Customer Table")
        print()
            
# loan function
    def loan_add(loan_details):
        sql4 = ('insert into loan(loanno,  amount, type, bcode, bno) ''values(:loanno,  :amount, :type, :bcode, :bno)')
        with connection.cursor() as cursor:
            cursor.executemany(sql4, loan_details)
            connection.commit()
        print("Loan Details added in Loan Table")
        print()

# account cutomer function
    def acc_cus_add(acc_cus_details):
        sql5 = ('insert into account_customer(ssn, acctno) ''values(:ssn,  :acctno)')
        with connection.cursor() as cursor:
            cursor.executemany(sql5, acc_cus_details)
            connection.commit()
        print("Account Customer Details added in Loan Table")
        print()

# loan cutomer function
    def loan_cus_add(loan_cus_details):
        sql6 = ('insert into loan_customer(ssn, loanno) ''values(:ssn,  :loanno)')
        with connection.cursor() as cursor:
            cursor.executemany(sql6, loan_cus_details)
            connection.commit()
        print("Loan Customer Details added in Loan Table")
            
            
    if __name__ == '__main__':
        
        
        # insert bank table details
        bank_add(bank_details)

        # insert bank_branch details
        bank_branch_add(bank_branch_details)

        # insert acount details
        account_add(Acount_details)

        # insert customer etails
        customer_add(Customer_details)
        
        # insert loan details
        loan_add(Loan_details)

        # insert account customer details
        acc_cus_add(Account_customer_details)

        # insert loan customer details
        loan_cus_add(Loan_customer_details)
        
        
       

       
        





