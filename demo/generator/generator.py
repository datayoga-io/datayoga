import time
from contextlib import suppress
from random import randrange
from sys import stderr

from faker import Faker
import httpx

TARGET = "http://datayoga:8080/"

fake = Faker()


def gen_fake_data():
    fname = fake.first_name()
    lname = fake.last_name()
    email = f"{fname.lower()}.{lname.lower()}@{fake.free_email_domain()}"

    return {
        "first_name": fname,
        "last_name": lname,
        "email": email
    }


def send_request(target: str = TARGET):
    data = gen_fake_data()

    with suppress(Exception):
        httpx.post(target, json=data)
        return True

    return False


def run():
    print(f"Sending fake data to {TARGET}", file=stderr)
    print("Press Ctrl+C to stop.", file=stderr)

    try:
        cnt = 0
        while True:
            if send_request():
                cnt += 1
                if cnt % 100 == 0:
                    print(f"Sent {cnt} requests so far...", file=stderr)
            time.sleep(randrange(3, 50) / 1000)
    except KeyboardInterrupt:
        print("Exiting...", file=stderr)

    print("Bye!", file=stderr)


if __name__ == '__main__':
    run()
