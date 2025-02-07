import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import mysql.connector
import uuid


DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "FHG*KLUk%mAq12",
    "database": "fraud_detection_db"
}

# Definir países y su clasificación de riesgo
location_risk = {
    "Ecuador": "Safe",
    "Peru": "Neutral",
    "Chile": "Neutral",
    "Colombia": "Neutral",
    "India": "High-Risk",
    "Indonesia": "High-Risk",
    "Brazil": "High-Risk"
}

merchants = [
    "Booking",
    "LATAM", 
    "Delta Airlines", 
    "American Airlines",
    "Steam", 
    "G2A", 
    "Epic Games", 
    "Amazon Gift Cards",
    "Zales", 
    "Tiffany & Co.", 
    "Cartier", 
    "Zephora", 
    "Amazon", 
    "Shein", 
    "eBay", 
    "Walmart" 
]

transaction_type = [
    "E-Commerce"
]

# Probabilidades de aparición de cada país
location_weights = {
    "Ecuador": 0.8, "Peru": 0.06, "Chile": 0.06, "Colombia": 0.05,
    "India": 0.01, "Indonesia": 0.01, "Brazil": 0.01
}

# Generar un país basado en las probabilidades
def generate_location():
    countries = list(location_weights.keys())
    weights = list(location_weights.values())
    return np.random.choice(countries, p=weights)

# Generar un ID único para transacciones y tarjetas
def generate_transaction_id():
    return 'T' + str(uuid.uuid4().int)[:10]

def generate_card_id():
    return 'C' + str(np.random.randint(10000, 99999))

# Generar monto de transacción aleatorio
def generate_transaction_amount():
    return round(np.random.uniform(1, 1000), 2)

# Generar monto promedio transaccionado por el cliente
def generate_avg_transaction_amount():
    return round(np.random.uniform(1,1000), 2)

# Generar numero de transacciones promedio de un cliente
def generate_avg_transaction_per_day():
    return np.random.randint(1,40)

# Generar monto promedio transaccionado por el cliente
def generate_accumulated_transaction_amount():
    return round(np.random.uniform(1,1000), 2)

# Generar numero de transacciones promedio de un cliente
def generate_accumulated_transaction_per_day():
    return np.random.randint(1,40)

def generate_merchant():
    return np.random.choice(merchants)

def generate_transaction_type():
    return np.random.choice(transaction_type)

# Generar transacción con posible fraude por localización y país de riesgo
def generate_transaction(prev_location=None, prev_time=None):
    transaction_id = generate_transaction_id()
    card_id = generate_card_id()
    transaction_amount = generate_transaction_amount()
    avg_transaction_amount = generate_avg_transaction_amount()
    avg_transaction_per_day = generate_avg_transaction_per_day()
    accumulated_transaction_amount = generate_accumulated_transaction_amount()
    accumulated_transaction_per_day = generate_accumulated_transaction_per_day()
    merchant = generate_merchant()
    transaction_type = generate_transaction_type()
    
    if prev_location is None:  # Primera transacción
        transaction_location = generate_location() if np.random.rand() > 0.05 else None
        time_difference = None
        is_fraudulent = 0
    else:
        # 80% de las veces la transacción es en el mismo país (legítima)
        if np.random.rand() < 0.8:
            transaction_location = prev_location 
        else:
            transaction_location = generate_location() if np.random.rand() > 0.05 else None
        
        # Generamos tiempo aleatorio entre 1 y 120 minutos
        transaction_time = prev_time + timedelta(minutes=np.random.randint(1, 120))
        time_difference = (transaction_time - prev_time).seconds / 60  if np.random.rand() > 0.02 else None
        # Reglas de fraude:
        prev_risk = location_risk[prev_location]
        if transaction_location is None:
             current_risk = "Unknown"
        else:
            current_risk = location_risk[transaction_location]


        if transaction_location != prev_location:
            if time_difference is not None and time_difference < 10: # Lógica de fraude:  
                is_fraudulent = 1  # Cambio de país en poco tiempo = FRAUDE
            elif current_risk == "High-Risk" and prev_risk != "High-Risk" and accumulated_transaction_amount >= avg_transaction_amount or accumulated_transaction_per_day >= avg_transaction_per_day:
                is_fraudulent = np.random.choice([0, 1], p=[0.7, 0.3])  # 30% de fraude si el país es de alto riesgo
            else:
                is_fraudulent = 0
        else:
            is_fraudulent = 0
    
    return {
        "Transaction_ID": transaction_id,
        "Card_ID": card_id,
        "Transaction_Amount": transaction_amount,
        "Merchant": merchant,
        "Transaction_Type": transaction_type,
        "Average_Transaction_Per_Day":avg_transaction_per_day,
        "Accumulated_Transactions": accumulated_transaction_per_day,
        "Average_Transaction_Amount": avg_transaction_amount,
        "Accumulated_Amount": accumulated_transaction_amount,
        "Transaction_Location": transaction_location,
        "Previous_Location": prev_location if prev_location else transaction_location,
        "Time_Difference (min)": time_difference if time_difference else 0,
        "Is_Fraudulent": is_fraudulent
    }

def insert_into_db(data):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    sql = """
        REPLACE INTO transactions (
            Transaction_ID, Card_ID, Transaction_Amount, Merchant, Transaction_Type, 
            Average_Transaction_Per_Day, Accumulated_Transactions, Average_Transaction_Amount, 
            Accumulated_Amount, Transaction_Location, Previous_Location, 
            Time_Difference, Is_Fraudulent
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """


    
    processed_data = []
    for row in data:
        cleaned_row = []
        for key in [
            "Transaction_ID", "Card_ID", "Transaction_Amount", "Merchant", "Transaction_Type",
            "Average_Transaction_Per_Day", "Accumulated_Transactions", "Average_Transaction_Amount",
            "Accumulated_Amount", "Transaction_Location", "Previous_Location",
            "Time_Difference (min)", "Is_Fraudulent"
        ]:
            value = row[key]  # Ahora accedemos correctamente a los valores del diccionario
            if isinstance(value, np.integer):
                cleaned_row.append(int(value))
            elif isinstance(value, np.floating):
                cleaned_row.append(float(value))
            elif isinstance(value, np.ndarray):
                cleaned_row.append(value.tolist() if value.size > 1 else value.item())
            else:
                cleaned_row.append(value)
        processed_data.append(cleaned_row)

    cursor.executemany(sql, processed_data)
    conn.commit()
    cursor.close()
    print("insertado")

def main():
    data = []
    num_transactions = 100000
    prev_location = None
    prev_time = datetime.now()

    for _ in range(num_transactions):
        transaction = generate_transaction(prev_location, prev_time)
        data.append(transaction)
        prev_location = transaction["Transaction_Location"]  # Transaction_Location
        prev_time = prev_time + timedelta(minutes=np.random.randint(1, 120))


    insert_into_db(data)

if __name__ == "__main__":
    main()