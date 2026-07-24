# Dictionary to store multiple customers
customers = {
    101: {
        "name": "Rahul",
        "balance": 5000,
        "transactions": []
    },
    102: {
        "name": "Priya",
        "balance": 7000,
        "transactions": []
    },
    103: {
        "name": "Aniket",
        "balance": 5000,
        "address": "mumbai",
        "transactions": []
    }

}


def create_account():
    customer_no = int(input("Enter Customer ID : "))

    if customer_no in customers:
        print("Customer ID Already Exists")
        return

    name = input("Enter Customer Name : ")
    balance = float(input("Enter Opening Balance : "))

    customers[customer_no] = {
        "name": name,
        "balance": balance,
        "transactions": []
    }

    print("Account Created Successfully")


def deposit():
    customer_id = int(input("Enter Customer ID : "))

    if customer_id not in customers:
        print("Customer Not Found")
        return

    amount = float(input("Enter Deposit Amount : "))

    if amount <= 0:
        print("Invalid Deposit Amount")
        return

    customers[customer_id]["balance"] += amount
    customers[customer_id]["transactions"].append(amount)

    print("Amount Deposited Successfully")


def withdraw():
    customer_id = int(input("Enter Customer ID : "))

    if customer_id not in customers:
        print("Customer Not Found")
        return

    amount = float(input("Enter Withdraw Amount : "))

    if amount <= 0:
        print("Negative Numbers Not Allowed")
        return

    if amount <= customers[customer_id]["balance"]:
        customers[customer_id]["balance"] -= amount
        customers[customer_id]["transactions"].append(-amount)
        print("Withdrawal Successful")
    else:
        print("Insufficient Balance")


def show_transactions():
    customer_id = int(input("Enter Customer ID : "))

    if customer_id not in customers:
        print("Customer Not Found")
        return

    print("Transaction History")

    if len(customers[customer_id]["transactions"]) == 0:
        print("No Transactions Found")
        return

    for t in customers[customer_id]["transactions"]:
        if t > 0:
            print("Deposit :", t)
        else:
            print("Withdrawal :", abs(t))


def show_balance():
    customer_id = int(input("Enter Customer ID : "))

    if customer_id not in customers:
        print("Customer Not Found")
        return

    print("Customer Name :", customers[customer_id]["name"])
    print("Current Balance :", customers[customer_id]["balance"])


while True:

    print("\nBSE BANK")
    print("1. Create Account")
    print("2. Deposit")
    print("3. Withdraw")
    print("4. Show Transactions")
    print("5. Show Balance")
    print("6. Exit")

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
        print("Thank You for Using BSE BANK")
        break

    else:
        print("Invalid Choice")
