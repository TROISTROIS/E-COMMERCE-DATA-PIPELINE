import csv
import random
from random import randint, choice
from faker import Faker
from faker.providers import company

fake = Faker()

# Define Customer IDS
customer_ids = [fake.unique.bothify(text='C#####') for _ in range(20)]
assert len(set(customer_ids)) == len(customer_ids)

membership_levels = ["Bronze", "Silver", "Gold", "Platinum"]
payment_method = ["Credit Card", "Debit Card","PayPal", "Bank Transfer", "Cash on Delivery", "Digital Wallet"]
delivery_status = ["Shipped", "Delivered", "Delayed", "Cancelled", "Returned"]

product_names = {
    'Electronics': ['Smartphone', 'Laptop', 'Tablet', 'Camera', 'Headphones'],
    'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Sneakers', 'Dress'],
    'Books': ['Novel', 'Biography', 'Science Fiction', 'Mystery', 'Non-Fiction'],
    'Home & Kitchen': ['Blender', 'Toaster', 'Vacuum Cleaner', 'Microwave', 'Cookware Set'],
    'Sports': ['Football', 'Tennis Racket', 'Basketball', 'Yoga Mat', 'Running Shoes']
}

#
# # Pick a random product and its category
# for _ in range(10):
#     category, product = pick_random_product()
#
#     print(f"Category: {category}, Product: {product}")

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
customer_names = {
    "first_names": [fake.unique.first_name() for _ in range(20)],
    "last_names": [fake.unique.last_name() for _ in range(20)]
}

# Ensure the generated names are unique
assert len(set(customer_names["first_names"])) == len(customer_names["first_names"])
assert len(set(customer_names["last_names"])) == len(customer_names["last_names"])

def get_product_price(product_id):
    """Retrieves the price for a given product ID from the pre-defined dictionary"""
    if product_id in product_with_prices:
        return product_with_prices[product_id]
    else:
        return 0.00
# suppliers
supplier_ids = []
for _ in range(10):
    ids = fake.bothify(text='S###')
    supplier_ids.append(ids)
assert len(set(supplier_ids)) == len(supplier_ids)

def generate_one_transaction(customer_id, current_date):
    if customer_id not in transaction_information:

        first_name = random.choice(customer_names["first_names"])
        last_name = random.choice(customer_names["last_names"])
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name().lower()}"
        # generate information needed
        transaction_information[customer_id] = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "membership_level": choice(membership_levels)
        }
    transaction_data = transaction_information[customer_id].copy()
    transaction_data["customer_id"] = customer_id
    transaction_data["transaction_id"] = fake.bothify(text='TXN#########')
    transaction_data["quantity"] = randint(1, 5)
    transaction_data["payment_type"] = choice(payment_method)
    transaction_data["status"] = choice(delivery_status)
    transaction_date = str(current_date)
    transaction_data["product_id"] = choice(products)
    price = get_product_price(transaction_data["product_id"])
    transaction_data["price"] = round((price * transaction_data["quantity"]),2)
    transaction_data["date"] = transaction_date
    transaction_data["category"] = choice(list(product_names.keys()))
    transaction_data["product_name"] = choice(product_names[transaction_data["category"]])
    transaction_data["supplier_id"] = choice(supplier_ids)

    return transaction_data

def generate_transactions(num_transactions, current_date):
    transactions = []
    for _ in range(num_transactions):
        transactions.append(generate_one_transaction(choice(customer_ids), current_date))
    return transactions

def write_to_csv(data, filename):
    with open(filename, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=['customer_id', 'transaction_id', 'first_name', 'last_name',
                                                  'email', 'membership_level','product_id', 'product_name',
                                                  'category', 'quantity', 'price', 'supplier_id',
                                                  'date', 'payment_type', 'status'])
        writer.writeheader()
        writer.writerows(data)
    return

fieldnames=["transaction_id", "customer_id", "product_id",
             "quantity", "price", "date",
              "payment_type", "status"]
def write_to_csv_n(data, filename):
    with open(filename, "w", newline="") as filen:

        writer = csv.DictWriter(filen, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
    return

def write_to_csv2(data, filename):
    with open(filename, "w", newline="") as file1:
        writer = csv.DictWriter(file1, fieldnames=["product_id", "product_name", "category",
                                                  "price", "supplier_id"], extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
    return

def write_to_csv3(data, filename):
    with open(filename, "w", newline="") as file2:
        writer = csv.DictWriter(file2, fieldnames=["customer_id", "first_name", "last_name",
                                                  "email", "membership_level"], extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
    return

def generate_data(current_date, date_str):
    transactions = generate_transactions(transactions_per_day, current_date)
    write_to_csv(transactions, f"/tmp/full_transactions.csv")
    write_to_csv_n(transactions, f"/tmp/transactions_{date_str}.csv")
    write_to_csv2(transactions, f"/tmp/dim_products.csv")
    write_to_csv3(transactions, f"/tmp/dim_customers.csv")
    print(f"Generated mock transaction data transactions_{date_str}.csv and saved in csv files")
    return
# working
