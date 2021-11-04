from faker import Faker

fake = Faker()
Faker.seed(0)


def name_address_generator():
    attempts = 0
    while attempts < 4:
        try:
            s = fake.name() + '\n' + fake.address()
            name, street_number, rest = s.split("\n")
            city, rest = rest.split(",")
            state, zip_code = rest.strip().split()
            return s
        except Exception:
            attempts += 1

    return "First Middle Last\n123 Snickersnack Lane\nBrooklyn, NY 11229"
