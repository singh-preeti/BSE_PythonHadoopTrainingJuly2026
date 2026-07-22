customer_name =""
balance = 0


def create_account():
    global customer_name

    customer_name = input("Enter Customer Name : ")

    print("Account Created Successfully")


def deposit():

    global balance

    amount = float(input("Enter Deposit Amount : "))

    balance = balance + amount

    print("Amount Deposited")


def withdraw():

    global balance

    amount = float(input("Enter Withdraw Amount : "))

    if amount <= balance:

        balance = balance - amount

        print("Withdrawal Successful")

    else:

        print("Insufficient Balance")

    if amount <= 0:
        print("Negative Numbers Not Allowed")


def show_balance():

    print("Current Balance :", balance)


while True:

    print("\nBSE BANK")
    print("1.Create Account")
    print("2.Deposit")
    print("3.Withdraw")
    print("4.Show Balance")
    print("5.Exit")

    choice = int(input("Enter Choice : "))

    if choice == 1:
        create_account()

    elif choice == 2:
        deposit()

    elif choice == 3:
        withdraw()

    elif choice == 4:
        show_balance()

    elif choice == 5:
        break
