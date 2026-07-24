import logging

# Configure Logging
logging.basicConfig(
    filename="bank.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

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
account_numbers = {101, 102}

# Transaction ID
transaction_id = 1001


def create_account():
    try:
        account_no = int(input("Enter Account Number : "))

        if account_no in account_numbers:
            print("Account Number Already Exists")
            logging.warning(f"Duplicate Account Number: {account_no}")
            return

        name = input("Enter Customer Name : ")
        balance = float(input("Enter Opening Balance : "))

        customers[account_no] = {
            "name": name,
            "balance": balance,
            "transactions": []
        }

        account_numbers.add(account_no)

        print("Account Created Successfully")
        logging.info(f"Account Created: {account_no} - {name}")

    except ValueError:
        print("Invalid Input")
        logging.error("Invalid input while creating account.")


def deposit():
    global transaction_id

    try:
        account_no = int(input("Enter Account Number : "))

        if account_no not in customers:
            print("Customer Not Found")
            return

        amount = float(input("Enter Deposit Amount : "))

        if amount <= 0:
            print("Invalid Deposit Amount")
            return

        date = input("Enter Date (DD-MMM-YYYY): ")

        customers[account_no]["balance"] += amount

        transaction = (transaction_id, date, amount)
        customers[account_no]["transactions"].append(transaction)

        logging.info(f"Deposit: Account {account_no}, Amount ₹{amount}")

        transaction_id += 1

        print("Amount Deposited Successfully")

    except ValueError:
        print("Invalid Input")
        logging.error("Invalid input during deposit.")


def withdraw():
    global transaction_id

    try:
        account_no = int(input("Enter Account Number : "))

        if account_no not in customers:
            print("Customer Not Found")
            return

        amount = float(input("Enter Withdraw Amount : "))

        if amount <= 0:
            print("Negative Numbers Not Allowed")
            return

        if amount <= customers[account_no]["balance"]:

            date = input("Enter Date (DD-MMM-YYYY): ")

            customers[account_no]["balance"] -= amount

            transaction = (transaction_id, date, -amount)

            customers[account_no]["transactions"].append(transaction)

            logging.info(f"Withdrawal: Account {account_no}, Amount ₹{amount}")

            transaction_id += 1

            print("Withdrawal Successful")

        else:
            print("Insufficient Balance")
            logging.warning(f"Insufficient Balance: Account {account_no}")

    except ValueError:
        print("Invalid Input")
        logging.error("Invalid input during withdrawal.")


def show_transactions():
    try:
        account_no = int(input("Enter Account Number : "))

        if account_no not in customers:
            print("Customer Not Found")
            return

        print("\nTransaction History")

        if len(customers[account_no]["transactions"]) == 0:
            print("No Transactions Found")
            return

        for t in customers[account_no]["transactions"]:
            if t[2] > 0:
                print("ID:", t[0], "| Date:", t[1], "| Deposit:", t[2])
            else:
                print("ID:", t[0], "| Date:", t[1], "| Withdrawal:", abs(t[2]))

    except ValueError:
        print("Invalid Input")
        logging.error("Invalid input while viewing transactions.")


def show_balance():
    try:
        account_no = int(input("Enter Account Number : "))

        if account_no not in customers:
            print("Customer Not Found")
            return

        print("Customer Name :", customers[account_no]["name"])
        print("Current Balance :", customers[account_no]["balance"])

    except ValueError:
        print("Invalid Input")
        logging.error("Invalid input while checking balance.")


def apply_cashback():
    try:
        account_no = int(input("Enter Account Number : "))

        if account_no not in customers:
            print("Customer Not Found")
            return

        cashback = list(
            map(
                lambda x: (x[0], x[1], x[2] + 100),
                customers[account_no]["transactions"]
            )
        )

        print("\nTransactions After ₹100 Cashback")

        for t in cashback:
            print(t)

        logging.info(f"Cashback Applied (Preview): Account {account_no}")

    except ValueError:
        print("Invalid Input")
        logging.error("Invalid input during cashback.")


def show_fraud_transactions():
    try:
        account_no = int(input("Enter Account Number : "))

        if account_no not in customers:
            print("Customer Not Found")
            return

        fraud_transactions = list(
            filter(
                lambda x: abs(x[2]) > 50000,
                customers[account_no]["transactions"]
            )
        )

        print("\nFraud Transactions (> ₹50000)")

        if len(fraud_transactions) == 0:
            print("No Fraud Transactions Found")
            return

        for t in fraud_transactions:
            print("Transaction ID:", t[0], "| Date:", t[1], "| Amount:", t[2])

        logging.warning(f"Fraud Check Performed: Account {account_no}")

    except ValueError:
        print("Invalid Input")
        logging.error("Invalid input during fraud detection.")


while True:
    try:
        print("\n========== BSE BANK ==========")
        print("1. Create Account")
        print("2. Deposit")
        print("3. Withdraw")
        print("4. Show Transactions")
        print("5. Show Balance")
        print("6. Apply ₹100 Cashback")
        print("7. Detect Fraud Transactions")
        print("8. Exit")

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
            apply_cashback()

        elif choice == 7:
            show_fraud_transactions()

        elif choice == 8:
            print("Thank You for Using BSE BANK")
            logging.info("Application Closed")
            break

        else:
            print("Invalid Choice")

    except ValueError:
        print("Please enter a valid numeric choice.")
        logging.error("Invalid menu choice entered.")

    except Exception as e:
        print("Unexpected Error:", e)
        logging.exception("Unexpected Exception")
