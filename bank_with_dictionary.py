customer_name =""
balance = 0

#list to store the transaction history
transactions = []


def create_account():
    global customer_name

    customer_name = input("Enter Customer Name : ")

    print("Account Created Successfully")


def deposit():

    global balance
    global transactions

    amount = float(input("Enter Deposit Amount : "))

    balance += amount
    transactions.append(amount)

    print("Amount Deposited")


def withdraw():

    global balance
    global transactions

    amount = float(input("Enter Withdraw Amount : "))

    if amount <= balance:

        balance -= amount
        transactions.append(-amount)

        print("Withdrawal Successful")

    else:

        print("Insufficient Balance")

    if amount <= 0:
        print("Negative Numbers Not Allowed")

def show_transactions():
    print("Transaction History")

    if len(transactions) ==0:
        print("No Transactions Found")
        return
    for t in transactions:
        if t>0:
            print("deposit:",t)
        else:
            print("withdrawal:",t)





def show_balance():

    print("Current Balance :", balance)


while True:

    print("\nBSE BANK")
    print("1.Create Account")
    print("2.Deposit")
    print("3.Withdraw")
    print("4.Show Transactions")
    print("5.Show Balance")
    print("6.Exit")

    choice = int(input("Enter Choice : "))

    if choice == 1:
        create_account()

    elif choice == 2:
        deposit()

    elif choice == 3:
        withdraw()

    elif choice == 4:
        show_transactions()

    elif choice == 5:
        show_balance()

    elif choice == 6:
        break
