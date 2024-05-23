import csv
import random
from random import randint, choice
from faker import Faker

fake = Faker()

# Define Customer IDS
customer_ids = []
for _ in range(20):
    ids = fake.bothify(text='C#####')
    customer_ids.append(ids)
assert len(set(customer_ids)) == len(customer_ids)

payment_method = ["Credit Card", "Debit Card", "Cash", "Digital Wallet","Mobile Wallet"]
delivery_status = ["In Transit", "Delivered", "Delayed", "Cancelled"]


# Define the number of transactions per day
transactions_per_day = 17

transaction_information = {}

# List of video games for sale
products = []
for _ in range(20):
    prods = fake.bothify(text='P#####')
    products.append(prods)
assert len(set(products)) == len(products)

# Assign prices to each video game
product_with_prices = {product: round(random.uniform(20, 100), 2) for product in products}
#
# # Print the products with their prices
# for product, price in product_with_prices.items():
#     print(f"{product}: ${price}")

def get_product_price(product_id):
    """Retrieves the price for a given product ID from the pre-defined dictionary"""
    if product_id in product_with_prices:
        return product_with_prices[product_id]
    else:
        return 0.00

def generate_one_transaction(transaction_id, current_date):
    if transaction_id not in transaction_information:
        # generate information needed
        transaction_information[transaction_id] = {
            "customer_id" : choice(customer_ids)
        }
    transaction_data = transaction_information[transaction_id].copy()
    transaction_data["transaction_id"] = fake.bothify(text='TXN#########')
    transaction_data["quantity"] = randint(1,5)
    transaction_data["payment_type"] = choice(payment_method)
    transaction_data["status"] = choice(delivery_status)
    transaction_date = str(current_date)
    transaction_data["product_id"] = choice(products)
    price = get_product_price(transaction_data["product_id"])
    transaction_data["price"] = round((price * transaction_data["quantity"]),2)
    transaction_data["date"] = transaction_date
    return transaction_data

def generate_transactions(num_transactions, current_date):
    transactions = []
    for _ in range(num_transactions):
        transactions.append(generate_one_transaction(choice(customer_ids), current_date))
    return transactions

def write_to_csv(data, filename):
    with open(filename, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["transaction_id", "customer_id", "product_id",
                                                  "quantity","price","date",
                                                  "payment_type", "status"])
        writer.writeheader()
        writer.writerows(data)
    return

def generate_data(current_date, date_str):
    transactions = generate_transactions(transactions_per_day, current_date)
    write_to_csv(transactions, f"/tmp/transactions_{date_str}.csv")

    print(f"Generated mock transaction data transactions_{date_str}.csv and saved in csv files")
    return

