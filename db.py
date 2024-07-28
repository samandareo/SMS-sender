import psycopg2 as pg
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import threading
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import asyncio

# Authenticate with Google Sheets
creds = Credentials.from_service_account_file('extras/credentials.json')
service = build('sheets', 'v4', credentials=creds)

spreadsheet_id = "13rgsyFiA7URNNE2XNCFvIqkMdQzoBJXURiQBtVsoWqY"

global conn, cursor, stop_event, import_thread, scheduler

conn = pg.connect(
    host="dpg-cqitjr8gph6c738u4sp0-a.frankfurt-postgres.render.com",
    database="tarbotdb",
    user="sreo",
    password="4INdhZzCVyZ7GagHBnJoPp38sAqg3iOS",
    port="5432"
)

cursor = conn.cursor()
stop_event = threading.Event()
import_thread = None
scheduler = None

def get_row_count(tbname):
    try:
        cursor.execute(f"SELECT count FROM rows_count WHERE name = '{tbname}'")
        return cursor.fetchone()[0]
    except Exception as e:
        print(e)
        return None

async def all_users(sheet_name, table_name):
    counter = get_row_count(table_name)
    start_row = counter + 1
    print(get_row_count(table_name))
    print(f"Starting from row {start_row}")
    range_name = f'{sheet_name}!B{start_row}:D'
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    if not values:
        print(f'No new data found in {sheet_name}.')
        return
    else:
        try:
            for row in values:
                if len(row) >= 2:
                    cursor.execute(f"SELECT * FROM {table_name} WHERE phone_number = '{row[0]}' AND book_id = {row[2]}")
                    counter += 1
                    result = cursor.fetchone()
                    if result is None:
                        print(f"Adding {row[1]} ({row[0]}) with book id {row[2]} to the database.")
                        cursor.execute(f"INSERT INTO {table_name} (name, phone_number, book_id) VALUES (%s, %s, %s)", (row[1], row[0], row[2]))
                        conn.commit()
                    else:
                        print(f'{row[1]} already exists in the database with this book id {row[2]}.')
                await asyncio.sleep(2)
        except Exception as e:
            print(e)
        finally:
            cursor.execute(f"UPDATE rows_count SET count = {counter} WHERE name = '{table_name}'")
            conn.commit()

async def checked_users(sheet_name, table_name):
    counter = get_row_count(table_name)
    start_row = counter + 1
    range_name = f'{sheet_name}!B{start_row}:C'
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    if not values:
        print(f'No new data found in {sheet_name}.')
        return
    else:
        try:
            for row in values:
                counter += 1
                print(f"{row[1]}'s number is {row[0]}")
                if len(row) >= 2:
                    cursor.execute(f"SELECT * FROM {table_name} WHERE phone_number = '{row[0]}'")
                    result = cursor.fetchone()
                    if result is None:
                        print(f"Adding {row[1]} ({row[0]}) to the database.")
                        cursor.execute(f"INSERT INTO {table_name} (name, phone_number) VALUES (%s, %s)", (row[1], row[0]))
                        conn.commit()
                    else:
                        print(f'{row[1]} already exists in the database.')
                await asyncio.sleep(2)
        except Exception as e:
            print(e)
        finally:
            cursor.execute(f"UPDATE rows_count SET count = {counter} WHERE name = '{table_name}'")
            conn.commit()

async def google_sheets_imports():
    await all_users('All', 'users')
    await checked_users('Chack', 'end_users')

async def import_data():
    while not stop_event.is_set():
        await google_sheets_imports()
        await asyncio.sleep(60) 

def run_import_data():
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(import_data())
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()

def start_import():
    global import_thread, scheduler
    if import_thread is None or not import_thread.is_alive():
        stop_event.clear()
        import_thread = threading.Thread(target=run_import_data)
        import_thread.start()

def stop_import():
    global import_thread, loop, stop_event

    # Set the stop event to signal the thread to stop
    stop_event.set()

    if import_thread is not None:
        # Stop the asyncio event loop
        loop.call_soon_threadsafe(loop.stop)
        
        # Join the import thread to wait for it to terminate
        import_thread.join()

        print("Stopping loop...")
        print("Import thread stopped.")
        import_thread = None
        loop = None