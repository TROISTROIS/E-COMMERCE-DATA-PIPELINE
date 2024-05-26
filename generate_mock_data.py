import csv
import random
from random import randint, choice
from faker import Faker

fake = Faker()

# Define Customer IDS
customer_ids = ['C98796', 'C11370', 'C43891', 'C56493', 'C48310', 'C45282', 'C03014', 'C26165',
                'C03708', 'C80280', 'C67165', 'C33864', 'C78927', 'C60501', 'C56743', 'C74095',
                'C57289', 'C20167', 'C52001', 'C38698', 'C53088', 'C21608', 'C51388', 'C91479',
                'C43210', 'C63128', 'C31773', 'C00874', 'C40138', 'C73559', 'C70611', 'C55560',
                'C48051', 'C73868', 'C17438', 'C74641', 'C02527', 'C03140', 'C37814', 'C05673']

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


# Define the number of transactions per day
transactions_per_day = 20

transaction_information = {}

# List of video games for sale
products = ['P27675', 'P78718', 'P57166', 'P71758', 'P53184', 'P77619', 'P13500', 'P45728', 'P63773',
            'P34073', 'P84709', 'P56157', 'P91214', 'P88796', 'P98388', 'P98064', 'P10145', 'P95364',
            'P30910', 'P88761', 'P28637', 'P61843', 'P42988', 'P64509', 'P39651', 'P16193', 'P87851',
            'P84789', 'P31454', 'P58441', 'P79335', 'P86088', 'P40467', 'P42058', 'P28213', 'P22763',
            'P42894', 'P52797', 'P73971', 'P68472', 'P32543', 'P66616', 'P09232', 'P61865', 'P28043',
            'P15455', 'P19433', 'P92095', 'P67595', 'P17174', 'P54208', 'P79126', 'P68010', 'P54376',
            'P71777', 'P22919', 'P81116', 'P57423', 'P76938', 'P20111', 'P26976', 'P77782', 'P94982',
            'P89863', 'P05248', 'P11528', 'P71849', 'P65337', 'P05102', 'P51525', 'P05591', 'P40919',
            'P04960', 'P96519', 'P84696', 'P90100', 'P91078', 'P12023', 'P34232', 'P12813', 'P52505',
            'P95115', 'P74826', 'P90348', 'P35586', 'P47539', 'P13178', 'P27010', 'P65600', 'P54052',
            'P19495','P14648', 'P15736', 'P55498', 'P23140', 'P89803', 'P36174', 'P56672', 'P63055',
            'P72730']

# Assign prices to each product
product_with_prices = {product: round(random.uniform(20, 100), 2) for product in products}

#  Print the products with their prices
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

product_price_category = {}

for product_id in products:
    category = random.choice(list(product_names.keys()))
    product_name = random.choice(product_names[category])
    product_price_category[product_id] = {
        "category": category,
        "product_name": product_name,
        "price": get_product_price(product_id)
    }

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
    transaction_data["product_id"] = random.choice(products)
    transaction_data["category"] = product_price_category.get(transaction_data["product_id"], {}).get("category", None)
    price = product_price_category.get(transaction_data["product_id"], {}).get("price", None)
    transaction_data["product_name"] = product_price_category.get(transaction_data["product_id"], {}).get("product_name", None)
    transaction_data["price"] = round((price * transaction_data["quantity"]),2)
    transaction_data["date"] = transaction_date
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
