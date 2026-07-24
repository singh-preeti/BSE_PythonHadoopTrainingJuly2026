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
    }
}

# Set to store unique account numbers
account_numbers = {101, 102,103}


def create_account():
    account_no = int(input("Enter Account Number : "))

    # Check if account number already exists
    if account_no in account_numbers:
        print("Account Number Already Exists")
        return

    name = input("Enter Customer Name : ")
    balance = float(input("Enter Opening Balance : "))

    customers[account_no] = {
        "name": name,
        "balance": balance,
        "transactions": []
    }

    # Add account number to the set
    account_numbers.add(account_no)

    print("Account Created Successfully")


def deposit():
    account_no = int(input("Enter Account Number : "))

    if account_no not in customers:
        print("Customer Not Found")
        return

    amount = float(input("Enter Deposit Amount : "))

    if amount <= 0:
        print("Invalid Deposit Amount")
        return

    customers[account_no]["balance"] += amount
    customers[account_no]["transactions"].append(amount)

    print("Amount Deposited Successfully")


def withdraw():
    account_no = int(input("Enter Account Number : "))

    if account_no not in customers:
        print("Customer Not Found")
        return

    amount = float(input("Enter Withdraw Amount : "))

    if amount <= 0:
        print("Negative Numbers Not Allowed")
        return

    if amount <= customers[account_no]["balance"]:
        customers[account_no]["balance"] -= amount
        customers[account_no]["transactions"].append(-amount)
        print("Withdrawal Successful")
    else:
        print("Insufficient Balance")


def show_transactions():
    account_no = int(input("Enter Account Number : "))

    if account_no not in customers:
        print("Customer Not Found")
        return

    print("Transaction History")

    if len(customers[account_no]["transactions"]) == 0:
        print("No Transactions Found")
        return

    for t in customers[account_no]["transactions"]:
        if t > 0:
            print("Deposit :", t)
        else:
            print("Withdrawal :", abs(t))


def show_balance():
    account_no = int(input("Enter Account Number : "))

    if account_no not in customers:
        print("Customer Not Found")
        return

    print("Customer Name :", customers[account_no]["name"])
    print("Current Balance :", customers[account_no]["balance"])


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

