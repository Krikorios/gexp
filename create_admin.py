"""One-time script to create the initial admin user."""
import sys
import getpass
from database.schema import create_tables
from services.auth_service import create_user, get_user_by_username

def main():
    create_tables()
    
    username = input("Enter admin username: ").strip()
    if not username:
        print("Username cannot be empty.")
        sys.exit(1)
    
    if get_user_by_username(username):
        print(f"User '{username}' already exists.")
        sys.exit(1)
    
    password = getpass.getpass("Enter password: ")
    confirm = getpass.getpass("Confirm password: ")
    
    if password != confirm:
        print("Passwords do not match.")
        sys.exit(1)
    
    if len(password) < 8:
        print("Password must be at least 8 characters.")
        sys.exit(1)
    
    create_user(username, password)
    print(f"Admin user '{username}' created successfully.")

if __name__ == "__main__":
    main()
