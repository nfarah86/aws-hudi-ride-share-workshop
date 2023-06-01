"""

Author : Soumil Nitin Shah
"""

try:
    import os, sys, random, hashlib, datetime
    from faker import Faker

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute, NumberAttribute, UTCDateTimeAttribute
    from pynamodb.connection import Connection
    from pynamodb.exceptions import PutError
    from dotenv import load_dotenv

    load_dotenv("../Infrasture/.env")
except Exception as e:
    print("Some Modules are missing Error :{} ".format(e))

global faker
fake = Faker()


def generate_users(num_records):
    users = []
    for i in range(num_records):
        # Generate a unique user_id based on the hash of the name field
        name = fake.name()
        user_id = hashlib.sha256(name.encode()).hexdigest()

        # Add created_at field to the user dictionary
        created_at = datetime.datetime.now().isoformat()

        user = {
            "user_id": user_id,
            "name": name,
            "phone_number": fake.phone_number(),
            "email": fake.email(),
            "city": fake.city(),
            "state": fake.state(),
            "created_at": created_at
        }

        users.append(user)
    return users


def generate_rides(num_records, drivers, users):
    rides = []
    for i in range(num_records):
        ride = {
            "ride_id": i + 1,
            "user_id": random.choice(users)['user_id'],
            "driver_id": random.choice(drivers)['driver_id'],
            "ride_date": fake.date_between(start_date='-60d', end_date='today'),
            "ride_time": fake.time(pattern='%H:%M:%S', end_datetime=None),
            "pick_up_city": fake.city(),
            "pick_up_state": fake.state(),
            "pickup_location": fake.address(),
            "dropoff_location": fake.address(),
            "distance_travelled": round(random.uniform(0.5, 10.0), 2),
            "ride_duration": fake.time(pattern='%H:%M:%S', end_datetime=None),
            "fare": round(random.uniform(5.0, 100.0), 2)
        }
        rides.append(ride)
    return rides


def generate_tips(num_records, ride_ids):
    tips = []
    for i in range(num_records):
        tip = {
            "tip_id": i + 1,
            "ride_id": random.choice(ride_ids),
            "tip_amount": round(random.uniform(1.0, 20.0), 2)
        }
        tips.append(tip)
    return tips


def generate_drivers(num_records):
    car_makes = ["Toyota", "Honda", "Ford", "Tesla", "BMW", "Mercedes-Benz", "Audi", "Chevrolet", "Hyundai"]
    car_models = [
        ["Camry", "Corolla", "Prius", "Highlander", "RAV4"],
        ["Civic", "Accord", "CR-V", "Pilot", "Odyssey"],
        ["Mustang", "F-150", "Explorer", "Escape", "Focus"],
        ["Model S", "Model 3", "Model X", "Model Y", "Roadster"],
        ["3 Series", "5 Series", "7 Series", "X3", "X5"],
        ["C-Class", "E-Class", "S-Class", "GLC", "GLE"],
        ["A4", "A6", "Q5", "Q7", "TT"],
        ["Camaro", "Silverado", "Equinox", "Traverse", "Malibu"],
        ["Elantra", "Sonata", "Tucson", "Santa Fe", "Kona"]
    ]

    drivers = []
    for i in range(num_records):
        # Generate a unique driver_id based on the hash of the name field
        name = fake.name()
        driver_id = hashlib.sha256(name.encode()).hexdigest()

        car_make = random.choice(car_makes)
        car_model = random.choice(car_models[car_makes.index(car_make)])

        driver = {
            "driver_id": driver_id,
            "name": name,
            "phone_number": fake.phone_number(),
            "email": fake.email(),
            "city": fake.city(),
            "state": fake.state(),
            "car_make": car_make,
            "car_model": car_model,
            "license_plate": fake.license_plate(),
            "avg_rating": round(random.uniform(2.0, 5.0), 2),
            "num_ratings": random.randint(0, 100),
            "created_at": datetime.datetime.now().isoformat()
        }
        drivers.append(driver)
    return drivers


# ====================PYNAMODB CLASS============================
class RideModel(Model):
    class Meta:
        table_name = os.getenv("DYNAMO_DB_TABLE_NAME_RIDES")
        aws_access_key_id =os.getenv("DEV_ACCESS_KEY")
        aws_secret_access_key = os.getenv("DEV_SECRET_KEY")
        region = 'us-east-1'

    ride_id = UnicodeAttribute(hash_key=True)
    user_id = UnicodeAttribute()
    driver_id = UnicodeAttribute()
    ride_date = UnicodeAttribute()
    ride_time = UnicodeAttribute()
    pick_up_city = UnicodeAttribute()
    pick_up_state = UnicodeAttribute()
    pickup_location = UnicodeAttribute()
    dropoff_location = UnicodeAttribute()
    distance_travelled = UnicodeAttribute()
    ride_duration = UnicodeAttribute()
    fare = UnicodeAttribute()


class TipsModel(Model):
    class Meta:
        table_name = os.getenv("DYNAMO_DB_TABLE_NAME_TIPS")
        aws_access_key_id =os.getenv("DEV_ACCESS_KEY")
        aws_secret_access_key = os.getenv("DEV_SECRET_KEY")
        region = 'us-east-1'

    tip_id = UnicodeAttribute(hash_key=True)
    ride_id = UnicodeAttribute()
    tip_amount = UnicodeAttribute()


class DriverModel(Model):
    class Meta:
        table_name = os.getenv("DYNAMO_DB_TABLE_NAME_DRIVERS")
        aws_access_key_id =os.getenv("DEV_ACCESS_KEY")
        aws_secret_access_key = os.getenv("DEV_SECRET_KEY")
        region = 'us-east-1'  # Replace with your desired AWS region

    driver_id = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute()
    phone_number = UnicodeAttribute()
    email = UnicodeAttribute()
    city = UnicodeAttribute()
    state = UnicodeAttribute()
    car_make = UnicodeAttribute()
    car_model = UnicodeAttribute()
    license_plate = UnicodeAttribute()
    avg_rating = UnicodeAttribute()
    created_at = UnicodeAttribute()


class UserModel(Model):
    class Meta:
        table_name = os.getenv("DYNAMO_DB_TABLE_NAME_USERS")  # Replace with your desired table name
        aws_access_key_id =os.getenv("DEV_ACCESS_KEY")
        aws_secret_access_key = os.getenv("DEV_SECRET_KEY")
        region = 'us-east-1'  # Replace with your desired AWS region

    user_id = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute()
    phone_number = UnicodeAttribute()
    email = UnicodeAttribute()
    city = UnicodeAttribute()
    state = UnicodeAttribute()
    created_at = UnicodeAttribute()


# ====================Parameters ===========================
total_users = 2
total_drivers = 4
total_rides = 5
total_tips = 2
min_date = '2000-01-01'
max_date = '2025-01-01'
BUCKET_NAME = "jt-soumilshah-1995"
# =========================================================

users = generate_users(total_users)
drivers = generate_drivers(total_drivers)
rides = generate_rides(total_rides, drivers, users)
ride_ids = [ride['ride_id'] for ride in rides]
tips = generate_tips(total_tips, ride_ids)
# ===============================================================

connection = Connection()


# =================================================================
def ingest_rides(rides):
    for ride in rides:
        ride_instance = RideModel(
            ride_id=str(ride["ride_id"]),
            user_id=str(ride["user_id"]),
            driver_id=str(ride["driver_id"]),
            ride_date=str(ride["ride_date"]),
            ride_time=str(ride["ride_time"]),
            pick_up_city=str(ride["pick_up_city"]),
            pick_up_state=str(ride["pick_up_state"]),
            pickup_location=str(ride["pickup_location"]),
            dropoff_location=str(ride["dropoff_location"]),
            distance_travelled=str(ride["distance_travelled"]),
            ride_duration=str(ride["ride_duration"]),
            fare=str(ride["fare"])
        )

        try:
            ride_instance.save()
        except PutError as e:
            print(f"Error inserting ride {ride['ride_id']}: {str(e)}")


def ingest_tips(rides):
    for tip in tips:
        print("tip")
        tip_instance = TipsModel(
            tip_id=str(tip["tip_id"]),
            ride_id=str(tip["ride_id"]),
            tip_amount=str(tip["tip_amount"])
        )

        try:
            tip_instance.save()
        except PutError as e:
            print(f"Error inserting tip {tip['tip_id']}: {str(e)}")


def ingest_drivers(drivers):
    for driver in drivers:
        print("drivers")
        drivers_instance = DriverModel(
            driver_id=str(driver['driver_id']),
            name=str(driver['name']),
            phone_number=str(driver['phone_number']),
            email=str(driver['email']),
            city=str(driver['city']),
            state=str(driver['state']),
            car_make=str(driver['car_make']),
            car_model=str(driver['car_model']),
            license_plate=str(driver['license_plate']),
            avg_rating=str(driver['avg_rating']),
            created_at=str(driver['created_at'])
        )
        drivers_instance.save()


def ingest_users(users):
    for user in users:
        print("drivers")
        user_instance = UserModel(
            user_id=str(user['user_id']),
            name=str(user['name']),
            phone_number=str(user['phone_number']),
            email=str(user['email']),
            city=str(user['city']),
            state=str(user['state']),
            created_at=str(user['created_at'])
        )
        user_instance.save()


# ============================================================

print("Ingesting Rides ")
ingest_rides(rides)
print("Ingesting Tips ")
ingest_tips(tips)
print("Ingesting drivers ")
ingest_drivers(drivers)
print("Ingesting users ")
ingest_users(users)
