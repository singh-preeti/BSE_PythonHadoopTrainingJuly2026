# ---------------------------------------
# Customer Accounts
# ---------------------------------------

accounts = {

    "Rahul": 25000,
    "Amit": 8000,
    "Neha": 500,
    "Riya": 15000

}


# ---------------------------------------
# Reusable Payment Utility
# ---------------------------------------

def process_payment(customer, amount, payment_method):

    print("\n--------------------------------")
    print("Payment Method :", payment_method)
    print("Customer       :", customer)
    print("Amount         :", amount)

    # Step 1
    print("Step 1 : Validating Customer")

    if customer not in accounts:
        print("Payment Failed : Customer Not Found")
        return

    # Step 2
    print("Step 2 : Validating Amount")

    if amount <= 0:
        print("Payment Failed : Invalid Amount")
        return

    # Step 3
    print("Step 3 : Checking Account Balance")

    balance = accounts[customer]

    if balance < amount:

        print("Payment Failed : Insufficient Balance")
        print("Available :", balance)
        return

    # Step 4
    print("Step 4 : Deducting Amount")

    accounts[customer] -= amount

    # Step 5
    print("Step 5 : Payment Successful")

    print("Remaining Balance :", accounts[customer])

    process_payment("Rahul", 3000, "NEFT")

    process_payment("Amit", 1500, "GPAY")

    process_payment("Rahul", 4000, "CREDIT CARD")

    process_payment("Neha", 1000, "DEBIT CARD")

    process_payment("Riya", 2000, "WALLET")
