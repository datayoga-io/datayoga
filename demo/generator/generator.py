import time
from contextlib import suppress
from itertools import count
from random import randrange
from sys import stderr

import httpx
from faker import Faker

TARGET = "http://datayoga:8080/"

fake = Faker()


def fake_data_generator():
    counter = count()
    while True:
        fname = fake.first_name()
        lname = fake.last_name()
        email = f"{fname.lower()}.{lname.lower()}@{fake.free_email_domain()}"
        yield {
            "id": next(counter),
            "first_name": fname,
            "last_name": lname,
            "email": email,
            "active": fake.boolean(chance_of_getting_true=(int(time.time() / 100) % 30) + 70),
        }


def send_request(data: dict, target: str = TARGET):
    with suppress(Exception):
        httpx.post(target, json=data)
        return True

    return False


def run():
    print(f"Sending fake data to {TARGET}", file=stderr)
    print("Press Ctrl+C to stop.", file=stderr)

    try:
        counter = count()
        fake_data = fake_data_generator()

        while True:
            if not send_request(next(fake_data)):
                print("Failed to send request!", file=stderr)
                time.sleep(1)
                continue

            if (iteration := next(counter)) % 100 == 0:
                print(f"Sent {iteration} requests so far...", file=stderr)

            time.sleep(randrange(3, 50) / 1000)
    except KeyboardInterrupt:
        print("Exiting...", file=stderr)

    print("Bye!", file=stderr)


if __name__ == '__main__':
    run()
