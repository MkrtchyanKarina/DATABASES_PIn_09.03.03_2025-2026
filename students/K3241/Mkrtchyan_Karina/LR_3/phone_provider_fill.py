import psycopg2
import random
from faker import Faker
from datetime import datetime, timedelta, timezone

fake = Faker('ru_RU')

conn = psycopg2.connect(
    dbname="phone_provider",
    user="postgres",
    password="1QM2WM3EM4R",
    host="localhost",
    port="5432"
)
cur = conn.cursor()


def generate_valid_dates():
    modification = fake.date_this_decade()
    start = modification + timedelta(days=random.randint(1, 30))
    end = start + timedelta(days=random.randint(30, 365))
    return modification, start, end


# -------------------- 1. Subscribers --------------------
def gen_subscribers(n=150):
    for _ in range(n):
        dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
        issue_date = dob + timedelta(days=365*18)

        cur.execute("""
        INSERT INTO subscribers
        (name, lastname, surname, passport_series, passport_number,
         passport_issue_date, passport_issued_by, date_of_birth)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            fake.first_name(),
            fake.last_name(),
            fake.first_name(),
            str(random.randint(1000,9999)),
            str(random.randint(100000,999999)),
            issue_date,
            fake.company(),
            dob
        ))


# -------------------- 2. Contracts --------------------
def gen_contracts():
    cur.execute("SELECT subscriber_id FROM subscribers")
    subs = cur.fetchall()

    for s in subs:
        cur.execute("""
        INSERT INTO contracts (subscriber_id, phone_number, balance, date_start)
        VALUES (%s,%s,%s,%s)
        """, (
            s[0],
            f"+7-{random.randint(900,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            random.randint(0,10000),
            fake.date_time_this_decade()
        ))


# -------------------- 3. Tariffs --------------------
def gen_tariffs(n=50):
    for _ in range(n):
        cur.execute("""
        INSERT INTO tariffs (internet, sms, minutes, conditions, is_available)
        VALUES (%s,%s,%s,%s,%s)
        """, (
            round(random.uniform(1,100),2),
            random.randint(50,500),
            random.randint(100,2000),
            fake.text(50),
            True
        ))


# -------------------- 4. Tariff Prices --------------------
def gen_tariff_prices():
    cur.execute("SELECT tariff_id FROM tariffs")
    for (tid,) in cur.fetchall():
        modification, start, end = generate_valid_dates()
        cur.execute("""
        INSERT INTO tariffs_price
        (tariff_id, price, modification_date, start_date, expiration_date)
        VALUES (%s,%s,%s,%s,%s)
        """, (tid, random.randint(200,2000), modification, start, end))


# -------------------- 5. Connected Tariff --------------------
def gen_connected_tariffs():
    cur.execute("SELECT contract_id FROM contracts")
    contracts = cur.fetchall()
    cur.execute("SELECT tariff_id FROM tariffs")
    tariffs = cur.fetchall()

    for c in contracts:
        cur.execute("""
        INSERT INTO connected_tariff (contract_id, tariff_id, connection_date)
        VALUES (%s,%s,%s)
        """, (c[0], random.choice(tariffs)[0], fake.date_time_this_year()))


# -------------------- 6. Tariff Balances --------------------
def gen_tariff_balances():
    cur.execute("SELECT tariff_connection_id FROM connected_tariff")
    for (tcid,) in cur.fetchall():
        cur.execute("""
        INSERT INTO tariff_balances
        (tariff_connection_id, internet, sms, minutes)
        VALUES (%s,%s,%s,%s)
        """, (tcid, random.uniform(0,100), random.randint(0,200), random.randint(0,1000)))


# -------------------- 7. Tariff Debitings --------------------
def gen_tariff_debitings(n=200):
    cur.execute("SELECT tariff_connection_id FROM connected_tariff")
    ids = cur.fetchall()
    for _ in range(n):
        cur.execute("""
        INSERT INTO tariff_debitings
        (tariff_connection_id, date_time, amount)
        VALUES (%s,%s,%s)
        """, (random.choice(ids)[0], fake.date_time_this_year(), random.randint(10,500)))


# -------------------- 8. Resources --------------------
def gen_resources(n=50):
    devices = ["смартфон","планшет","роутер","умные часы","IoT"]
    for _ in range(n):
        cur.execute("""
        INSERT INTO resources (duration, device_type, conditions)
        VALUES (%s,%s,%s)
        """, (timedelta(days=random.randint(1,365)), random.choice(devices), fake.text(50)))


# -------------------- 9. Resources Price --------------------
def gen_resources_price():
    cur.execute("SELECT resource_id FROM resources")
    for (rid,) in cur.fetchall():
        modification, start, end = generate_valid_dates()
        cur.execute("""
        INSERT INTO resources_price
        (resource_id, price, modification_date, start_date, expiration_date)
        VALUES (%s,%s,%s,%s,%s)
        """, (rid, random.randint(50,1000), modification, start, end))


# -------------------- 10. Resources in Tariff --------------------
def gen_resources_in_tariff():
    cur.execute("SELECT tariff_id FROM tariffs")
    tariffs = cur.fetchall()
    cur.execute("SELECT resource_id FROM resources")
    resources = cur.fetchall()
    for _ in range(200):
        cur.execute("""
        INSERT INTO resources_in_tariff (tariff_id, resource_id)
        VALUES (%s,%s)
        """, (random.choice(tariffs)[0], random.choice(resources)[0]))


# -------------------- 11. Services --------------------
def gen_services(n=50):
    for _ in range(n):
        cur.execute("""
        INSERT INTO services (internet, sms, minutes, conditions, duration)
        VALUES (%s,%s,%s,%s,%s)
        """, (
            random.uniform(1,50),
            random.randint(10,200),
            random.randint(50,500),
            fake.text(50),
            timedelta(days=random.randint(1,90))
        ))


# -------------------- 12. Services Price --------------------
def gen_services_price():
    cur.execute("SELECT service_id FROM services")
    for (sid,) in cur.fetchall():
        modification, start, end = generate_valid_dates()
        cur.execute("""
        INSERT INTO services_price
        (service_id, price, modification_date, start_date, expiration_date)
        VALUES (%s,%s,%s,%s,%s)
        """, (sid, random.randint(50,1000), modification, start, end))


# -------------------- 13. Services in Tariff --------------------
def gen_services_in_tariff():
    cur.execute("SELECT tariff_id FROM tariffs")
    tariffs = cur.fetchall()
    cur.execute("SELECT service_id FROM services")
    services = cur.fetchall()
    for _ in range(200):
        cur.execute("""
        INSERT INTO services_in_tariff (tariff_id, service_id)
        VALUES (%s,%s)
        """, (random.choice(tariffs)[0], random.choice(services)[0]))


# -------------------- 14. Zones --------------------
def gen_zones(n=30):
    for _ in range(n):
        cur.execute("""
        INSERT INTO zones (country, region)
        VALUES (%s,%s)
        """, (fake.country(), fake.city()))


# -------------------- 15. Minute Price --------------------
def gen_minute_price():
    cur.execute("SELECT zone_id FROM zones")
    for (zid,) in cur.fetchall():
        modification, start, end = generate_valid_dates()
        cur.execute("""
        INSERT INTO minute_price
        (zone_id, price, modification_date, start_date, expiration_date)
        VALUES (%s,%s,%s,%s,%s)
        """, (zid, random.randint(1,50), modification, start, end))


# -------------------- 16. Call Log --------------------
def gen_calls(n=400):
    cur.execute("SELECT contract_id FROM contracts")
    contracts = cur.fetchall()
    cur.execute("SELECT zone_id FROM zones")
    zones = cur.fetchall()

    for _ in range(n):
        start = fake.date_time_this_year()
        end = start + timedelta(minutes=random.randint(1,60))

        cur.execute("""
        INSERT INTO call_log
        (contract_id, originator_zone_id, destination_zone_id,
         date_time_start, date_time_end, phone_number,
         number_type, is_incoming, total_cost)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            random.choice(contracts)[0],
            random.choice(zones)[0],
            random.choice(zones)[0],
            start, end,
            f"+7-{random.randint(900,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            random.choice(['мобильный','городской']),
            random.choice([True, False]),
            random.randint(1,500)
        ))


# -------------------- 17. Deposits --------------------
def gen_deposits(n=300):
    cur.execute("SELECT contract_id FROM contracts")
    contracts = cur.fetchall()

    for _ in range(n):
        cur.execute("""
        INSERT INTO deposits (contract_id, date_time, method, amount)
        VALUES (%s,%s,%s,%s)
        """, (
            random.choice(contracts)[0],
            fake.date_time_this_year(),
            random.choice(['банкомат','приложение оператора','смс-команда']),
            random.randint(100,5000)
        ))


# -------------------- 18. Connected Resources --------------------
def gen_connected_resources(n=200):
    cur.execute("SELECT contract_id FROM contracts")
    contracts = cur.fetchall()
    cur.execute("SELECT resource_id FROM resources")
    resources = cur.fetchall()

    for _ in range(n):
        start = fake.date_time_this_year()
        end = start + timedelta(days=random.randint(1,30))

        cur.execute("""
        INSERT INTO connected_resources
        (contract_id, resource_id, date_time_start, date_time_end)
        VALUES (%s,%s,%s,%s)
        """, (
            random.choice(contracts)[0],
            random.choice(resources)[0],
            start, end
        ))


# -------------------- 19. Connected Services --------------------
def gen_connected_services(n=200):
    cur.execute("SELECT contract_id FROM contracts")
    contracts = cur.fetchall()
    cur.execute("SELECT service_id FROM services")
    services = cur.fetchall()

    for _ in range(n):
        start = fake.date_time_this_year()
        end = start + timedelta(days=random.randint(1,30))

        cur.execute("""
        INSERT INTO connected_services
        (contract_id, service_id, date_time_start, date_time_end)
        VALUES (%s,%s,%s,%s)
        """, (
            random.choice(contracts)[0],
            random.choice(services)[0],
            start, end
        ))


# -------------------- 20. Service Balances --------------------
def gen_service_balances():
    cur.execute("SELECT service_connection_id FROM connected_services")
    for (sid,) in cur.fetchall():
        cur.execute("""
        INSERT INTO service_balances
        (service_connection_id, internet, sms, minutes)
        VALUES (%s,%s,%s,%s)
        """, (sid, random.uniform(0,50), random.randint(0,100), random.randint(0,300)))


# -------------------- 21. Service Debitings --------------------
def gen_service_debitings(n=200):
    cur.execute("SELECT service_connection_id FROM connected_services")
    ids = cur.fetchall()
    for _ in range(n):
        cur.execute("""
        INSERT INTO service_debitings
        (service_connection_id, date_time, amount)
        VALUES (%s,%s,%s)
        """, (random.choice(ids)[0], fake.date_time_this_year(), random.randint(10,500)))


# -------------------- 22. Resource Debitings --------------------
def gen_resource_debitings(n=200):
    cur.execute("SELECT resource_connection_id FROM connected_resources")
    ids = cur.fetchall()
    for _ in range(n):
        cur.execute("""
        INSERT INTO resources_debitings
        (resource_connection_id, date_time, amount)
        VALUES (%s,%s,%s)
        """, (random.choice(ids)[0], fake.date_time_this_year(), random.randint(10,500)))


# -------------------- RUN --------------------
print("Начинаем заполнение базы данных...")
print("=" * 50)

print("1. Заполнение subscribers...")
gen_subscribers()
print("   ✓ Готово")

print("2. Заполнение contracts...")
gen_contracts()
print("   ✓ Готово")

print("3. Заполнение tariffs...")
gen_tariffs()
print("   ✓ Готово")

print("4. Заполнение tariffs_price...")
gen_tariff_prices()
print("   ✓ Готово")

print("5. Заполнение connected_tariff...")
gen_connected_tariffs()
print("   ✓ Готово")

print("6. Заполнение tariff_balances...")
gen_tariff_balances()
print("   ✓ Готово")

print("7. Заполнение tariff_debitings...")
gen_tariff_debitings()
print("   ✓ Готово")

print("8. Заполнение resources...")
gen_resources()
print("   ✓ Готово")

print("9. Заполнение resources_price...")
gen_resources_price()
print("   ✓ Готово")

print("10. Заполнение resources_in_tariff...")
gen_resources_in_tariff()
print("   ✓ Готово")

print("11. Заполнение services...")
gen_services()
print("   ✓ Готово")

print("12. Заполнение services_price...")
gen_services_price()
print("   ✓ Готово")

print("13. Заполнение services_in_tariff...")
gen_services_in_tariff()
print("   ✓ Готово")

print("14. Заполнение zones...")
gen_zones()
print("   ✓ Готово")

print("15. Заполнение minute_price...")
gen_minute_price()
print("   ✓ Готово")

print("16. Заполнение call_log...")
gen_calls()
print("   ✓ Готово")

print("17. Заполнение deposits...")
gen_deposits()
print("   ✓ Готово")

print("18. Заполнение connected_resources...")
gen_connected_resources()
print("   ✓ Готово")

print("19. Заполнение connected_services...")
gen_connected_services()
print("   ✓ Готово")

print("20. Заполнение service_balances...")
gen_service_balances()
print("   ✓ Готово")

print("21. Заполнение service_debitings...")
gen_service_debitings()
print("   ✓ Готово")

print("22. Заполнение resources_debitings...")
gen_resource_debitings()
print("   ✓ Готово")

print("=" * 50)
conn.commit()
cur.close()
conn.close()

print("✅ ВСЕ 22 ТАБЛИЦЫ УСПЕШНО ЗАПОЛНЕНЫ!")