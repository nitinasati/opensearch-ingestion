import csv
import random
import uuid
from datetime import datetime, timedelta

def generate_csv_data(num_records=1000000, filename="testdata/member_data2_1000000.csv"):
    """Generates test data for the given Elasticsearch index mapping and saves it to a CSV file."""

    first_names = ["John", "Jane", "Michael", "Emily", "David", "Sarah", "Christopher", "Jessica", "Daniel", "Ashley", "Matthew", "Amanda", "Andrew", "Rebecca", "Joseph", "Samantha", "Joshua", "Elizabeth", "Ryan", "Stephanie"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]
    middle_names = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]
    countries = ["USA", "Canada", "UK", "Australia", "Germany", "France", "Japan", "China", "India", "Brazil"]
    genders = ["Male", "Female", "Other"]
    marital_statuses = ["Single", "Married", "Divorced", "Widowed"]
    employment_statuses = ["Employed", "Unemployed", "Retired", "Student"]
    member_statuses = ["Active", "Inactive", "Pending", "Terminated"]
    languages = ["English", "Spanish", "French", "German", "Chinese", "Japanese", "Arabic", "Russian", "Portuguese", "Hindi"]

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["memberId", "groupId", "firstName", "lastName", "middleName", "addressLine1", "addressLine2", "city", "state", "zipcode", "country", "phoneNumber1", "phoneNumber2", "email1", "email2", "objectId", "objectName", "subjectId", "subjectName", "fatherName", "motherName", "dateOfBirth", "gender", "maritalStatus", "employmentStatus", "policyNumber", "coverageStartDate", "coverageEndDate", "memberStatus", "preferredLanguage", "createdAt", "updatedAt"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for _ in range(num_records):
            print(f"Generating record {_} of {num_records}")    
            member_id = str(uuid.uuid4())
            group_id = str(uuid.uuid4())
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            middle_name = random.choice(middle_names)
            address_line1 = f"{random.randint(100, 999)} {random.choice(last_names)} St"
            address_line2 = f"Apt {random.randint(1, 50)}" if random.random() < 0.3 else None
            city = random.choice(cities)
            state = random.choice(states)
            zipcode = str(random.randint(10000, 99999))
            country = random.choice(countries)
            phone_number1 = f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            phone_number2 = f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}" if random.random() < 0.5 else None
            email1 = f"{first_name.lower()}.{last_name.lower()}{random.randint(1,100)}@example.com"
            email2 = f"{last_name.lower()}.{first_name.lower()}{random.randint(1,100)}@example.com" if random.random() < 0.4 else None
            object_id = str(uuid.uuid4())
            object_name = f"Object {random.randint(1, 1000)}"
            subject_id = str(uuid.uuid4())
            subject_name = f"Subject {random.randint(1, 1000)}"
            father_name = f"{random.choice(first_names)} {random.choice(last_names)}"
            mother_name = f"{random.choice(first_names)} {random.choice(last_names)}"
            start_date = datetime.now() - timedelta(days=random.randint(365 * 18, 365 * 60))
            date_of_birth = start_date.strftime("%Y-%m-%d")
            gender = random.choice(genders)
            marital_status = random.choice(marital_statuses)
            employment_status = random.choice(employment_statuses)
            policy_number = f"POL{random.randint(100000, 999999)}"
            coverage_start_date = (datetime.now() - timedelta(days=random.randint(1, 365 * 10))).strftime("%Y-%m-%d")
            coverage_end_date = (datetime.now() + timedelta(days=random.randint(1, 365 * 10))).strftime("%Y-%m-%d")
            member_status = random.choice(member_statuses)
            preferred_language = random.choice(languages)
            created_at = datetime.now().isoformat()
            updated_at = datetime.now().isoformat()

            record = {
                "memberId": member_id,
                "groupId": group_id,
                "firstName": first_name,
                "lastName": last_name,
                "middleName": middle_name,
                "addressLine1": address_line1,
                "addressLine2": address_line2,
                "city": city,
                "state": state,
                "zipcode": zipcode,
                "country": country,
                "phoneNumber1": phone_number1,
                "phoneNumber2": phone_number2,
                "email1": email1,
                "email2": email2,
                "objectId": object_id,
                "objectName": object_name,
                "subjectId": subject_id,
                "subjectName": subject_name,
                "fatherName": father_name,
                "motherName": mother_name,
                "dateOfBirth": date_of_birth,
                "gender": gender,
                "maritalStatus": marital_status,
                "employmentStatus": employment_status,
                "policyNumber": policy_number,
                "coverageStartDate": coverage_start_date,
                "coverageEndDate": coverage_end_date,
                "memberStatus": member_status,
                "preferredLanguage": preferred_language,
                "createdAt": created_at,
                "updatedAt": updated_at
            }
            writer.writerow(record)

    print(f"Generated {num_records} records and saved to {filename}")

if __name__ == "__main__":
    generate_csv_data()