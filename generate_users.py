import csv
import random
import string

def generate_random_username():
    """Generate a random username with 6-12 characters"""
    length = random.randint(6, 12)
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def generate_random_email(username):
    """Generate a random email based on username"""
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'company.com',
               'example.org', 'test.net', 'demo.io', 'mail.com', 'email.com']
    domain = random.choice(domains)
    # Sometimes use the username as-is, sometimes add numbers
    if random.random() < 0.7:
        email_prefix = username
    else:
        email_prefix = username + str(random.randint(1, 999))
    return f"{email_prefix}@{domain}"

def generate_users_csv(filename, num_users=5000):
    """Generate CSV file with random users"""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['userId', 'username', 'email']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header
        writer.writeheader()

        # Generate users
        for user_id in range(1, num_users + 1):
            username = generate_random_username()
            email = generate_random_email(username)

            writer.writerow({
                'userId': user_id,
                'username': username,
                'email': email
            })

    print(f"Generated {num_users} users in {filename}")

if __name__ == "__main__":
    # Generate the CSV file
    output_file = "src/main/resources/files/users.csv"
    generate_users_csv(output_file, 5000)
