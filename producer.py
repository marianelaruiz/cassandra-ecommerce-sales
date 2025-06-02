from kafka import KafkaProducer
from faker import Faker
import json
from datetime import datetime
import time

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_order():
    # List of home appliance products
    electronic_items = [
        "Laptop", "Smartphone", "Tablet", "Smartwatch", "Headphones", 
        "Bluetooth Speaker", "Television", "Microwave", "Refrigerator", 
        "Washing Machine", "Air Conditioner", "Electric Kettle", 
        "Coffee Maker", "Dishwasher", "Vacuum Cleaner"
    ]
    
    # Creating false data with home appliance products
    products = [{'name': fake.random_element(electronic_items),
                'quantity': fake.random_int(min=1, max=3),
                'price': fake.random_int(min=750, max=3000)}
                for _ in range(3)]     # We generate 3 products for each entry
    return {
        'order_id': fake.uuid4(),
        'customer_document': fake.ssn(),
        'products': products,
        'total_value': round(fake.random_number(digits=5) / 100, 2),
        'sale_date': datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    }

while True:
    sale = generate_order()
    print(f"Sending sale: {sale}")
    producer.send('sales-events', sale)
    time.sleep(5)
