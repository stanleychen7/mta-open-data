import database
from database import (create_tables, insert, update_table, 
                     alter_table, close, fetch_and_process)

def main():
    create_tables()

    data = fetch_and_process()

    insert(data)

    update_table()

    alter_table()

    close()

if __name__ =="__main__":
    main()