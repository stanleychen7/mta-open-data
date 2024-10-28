from database import select_Data
import datetime

def get_user_dates():
    while True:
        try:
            start_date_str = input("Enter the start date (YYYY-MM-DD HH:MM:SS): ")
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S").date()
            
            end_date_str = input("Enter the end date (YYYY-MM-DD HH:MM:SS): ")
            end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S").date()
            
            if end_date >= start_date:
                return start_date, end_date
            else:
                print("Error: End date must be greater than or equal to start date.")
        except ValueError:
            print("Invalid date format. Please use the format YYYY-MM-DD HH:MM:SS.")

def main():
    start_date, end_date = get_user_dates()
    select_Data(start_date, end_date)

if __name__ == "__main__":
    main()