import pymongo
import openai
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from threading import Semaphore
import csv

uri = ""

client = pymongo.MongoClient(uri)
db  = client.emails
collection  = db.embeddedEmails


openai.api_key = ''

class RateLimiter:
    def __init__(self, calls_per_minute):
        self.calls_per_minute = calls_per_minute
        self.semaphore = Semaphore(calls_per_minute)
        self.time_reset = time.time() + 60

    def wait_and_acquire(self):
        with self.semaphore:
            current_time = time.time()
            if current_time >= self.time_reset:
                self.semaphore = Semaphore(self.calls_per_minute)  # Reset the semaphore for the new minute
                self.time_reset = current_time + 60
            time_to_wait = self.time_reset - current_time
            # Calculate sleep time to distribute remaining calls over the time left in the minute
            sleep_time = time_to_wait / self.semaphore._value if self.semaphore._value > 0 else time_to_wait
            time.sleep(sleep_time)


def get_embedding(text,):
    # rate_limiter.wait_and_acquire()
    response = openai.embeddings.create(
        input=text,
        model="text-embedding-ada-002",
    )
    if response and response.data:
        embedding_data = response.data[0].embedding
        if embedding_data:
            return embedding_data
        else:
            raise Exception("No data found in response")
    else:
        raise Exception("Failed to get embedding")

def insert_email(email, rate_limiter):
    try:
        email['email_embedding'] = get_embedding(email['message'], rate_limiter)
        collection.insert_one(email)
        print(f"Updated:")
    except Exception as e:
        print(f"Failed to update {email['email']}: {e}")

def read_emails():
    emails = []
    with open('emails.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            emails.append(row)
    return emails

# rate_limiter = RateLimiter(3000)

# emails = read_emails()

# with ThreadPoolExecutor(max_workers=100) as executor:
#     futures = [executor.submit(insert_email, email, rate_limiter) for email in emails]
#     for future in as_completed(futures):
#         future.result()

# print("All emails have been embedded and inserted into the database")

path_to_vector = "email_embedding"

res = collection.aggregate([
    {"$vectorSearch": {
        "queryVector": get_embedding("email from frank f zerilli"),
        "path": path_to_vector,
        "numCandidates": 4000,
        "limit": 1,
        "index": "email",
    }}   
])

for i in res:
    print(f"file: {i['file']}, \n message: {i['message']}")