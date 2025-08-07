import csv
import random
import datetime
import json
from datetime import timedelta

def generate_random_message():
    """Generate a random message for testing notifications"""

    # Message types
    message_types = ['info', 'warning', 'error', 'success', 'alert']

    # Sample message templates
    message_templates = [
        "User {user} has logged in successfully",
        "System backup completed for database {db}",
        "Error processing request for user {user}: {error}",
        "New order #{order_id} received from customer {customer}",
        "Server {server} is experiencing high CPU usage: {cpu}%",
        "Payment of ${amount} processed successfully for user {user}",
        "Failed login attempt from IP {ip} for user {user}",
        "System maintenance scheduled for {date}",
        "New user registration: {user} - {email}",
        "Memory usage alert: {memory}% on server {server}",
        "File upload completed: {filename} ({size} MB)",
        "Database connection pool exhausted on {server}",
        "API rate limit exceeded for client {client}",
        "Scheduled task '{task}' completed successfully",
        "Configuration change detected in module {module}",
        "Email notification sent to {recipients} recipients",
        "Cache cleared for region {region}",
        "Security scan completed: {issues} issues found",
        "Batch job #{job_id} processing {records} records",
        "Service {service} restarted due to memory leak"
    ]

    # Random data for message formatting
    users = ['alice', 'bob', 'charlie', 'diana', 'eve', 'frank', 'grace', 'henry']
    servers = ['web-01', 'web-02', 'db-01', 'db-02', 'cache-01', 'api-01']
    databases = ['users_db', 'orders_db', 'analytics_db', 'logs_db']
    errors = ['timeout', 'invalid_credentials', 'network_error', 'permission_denied']
    services = ['auth-service', 'payment-service', 'notification-service', 'user-service']

    # Select random template and fill with data
    template = random.choice(message_templates)

    # Generate random data for placeholders
    data = {
        'user': random.choice(users),
        'db': random.choice(databases),
        'error': random.choice(errors),
        'order_id': random.randint(10000, 99999),
        'customer': random.choice(users),
        'server': random.choice(servers),
        'cpu': random.randint(60, 99),
        'amount': round(random.uniform(10.0, 999.99), 2),
        'ip': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        'date': (datetime.datetime.now() + timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d'),
        'email': f"{random.choice(users)}@example.com",
        'memory': random.randint(70, 95),
        'filename': f"document_{random.randint(1000, 9999)}.pdf",
        'size': round(random.uniform(0.5, 50.0), 1),
        'client': f"client_{random.randint(100, 999)}",
        'task': random.choice(['backup', 'cleanup', 'sync', 'report']),
        'module': random.choice(['auth', 'billing', 'inventory', 'analytics']),
        'recipients': random.randint(10, 500),
        'region': random.choice(['us-east', 'us-west', 'eu-central', 'asia-pacific']),
        'issues': random.randint(0, 15),
        'job_id': random.randint(1000, 9999),
        'records': random.randint(100, 10000),
        'service': random.choice(services)
    }

    # Format the message (handle missing placeholders gracefully)
    try:
        message = template.format(**data)
    except KeyError:
        message = template

    return {
        'id': None,  # Will be set by the loop
        'type': random.choice(message_types),
        'message': message,
        'timestamp': datetime.datetime.now() - timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        ),
        'severity': random.choice(['low', 'medium', 'high', 'critical']),
        'source': random.choice(['web-app', 'mobile-app', 'api', 'system', 'cron-job']),
        'user_id': random.randint(1, 5000)  # Reference to the users we would generate
    }

def generate_messages_csv(filename, num_messages=10000):
    """Generate CSV file with random messages"""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'type', 'message', 'timestamp', 'severity', 'source', 'user_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header
        writer.writeheader()

        # Generate messages
        for msg_id in range(1, num_messages + 1):
            message_data = generate_random_message()
            message_data['id'] = msg_id

            # Format timestamp as string
            message_data['timestamp'] = message_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

            writer.writerow(message_data)

    print(f"Generated {num_messages} messages in {filename}")

def generate_messages_json(filename, num_messages=10000):
    """Generate JSON file with random messages"""
    messages = []

    for msg_id in range(1, num_messages + 1):
        message_data = generate_random_message()
        message_data['id'] = msg_id

        # Format timestamp as ISO string
        message_data['timestamp'] = message_data['timestamp'].isoformat()

        messages.append(message_data)

    with open(filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(messages, jsonfile, indent=2, ensure_ascii=False)

    print(f"Generated {num_messages} messages in {filename}")

if __name__ == "__main__":
    # Generate both CSV and JSON formats
    csv_file = "src/main/resources/files/messages.csv"
    json_file = "src/main/resources/files/messages.json"

    generate_messages_csv(csv_file, 10000)
    generate_messages_json(json_file, 10000)
