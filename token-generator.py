import asyncpg
import asyncio
import requests
import json

DATABASE_CONFIG = {
    'host': "dpg-cqitjr8gph6c738u4sp0-a.frankfurt-postgres.render.com",
    'database': "tarbotdb",
    'user': "sreo",
    'password': "4INdhZzCVyZ7GagHBnJoPp38sAqg3iOS",
    'port': "5432"
}


async def get_db_connection():
    conn = await asyncpg.connect(
        user=DATABASE_CONFIG['user'],
        password=DATABASE_CONFIG['password'],
        database=DATABASE_CONFIG['database'],
        host=DATABASE_CONFIG['host'],
        port=DATABASE_CONFIG['port']
    )
    return conn


async def fetch_query(query, params=None):
    conn = await get_db_connection()
    result = await conn.fetch(query, *params if params else [])
    await conn.close()
    return result

async def execute_query(query, params=None):
    conn = await get_db_connection()
    await conn.execute(query, *params if params else [])
    await conn.close()

async def new_token(token):
    response = requests.patch('https://notify.eskiz.uz/api/auth/refresh',headers={'Authorization': f'Bearer {token}'})
    if response.status_code == 200:
        new_token = response.json().get('data').get('token')
        await execute_query(f"UPDATE creds SET value = '{new_token}' WHERE name = 'token'")
        print('Token updated.')


async def make_request(name, phone_number, text, link):
    token = await fetch_query("SELECT value FROM creds WHERE name = 'token'")
    url = 'https://notify.eskiz.uz/api/message/sms/send'
    headers = {
        'Authorization': f'Bearer {token[0]["value"]}',
    }
    data = {
        'mobile_phone': phone_number,
        'message': "Bu Eskiz dan test",
        'from': "4546",
        'callback_url': "http://0000.uz/test.php"
    }

    response = requests.post(url,headers=headers, data=data)
    print(response.json())
    return response.json()

# async def report():
#     token = await fetch_query("SELECT value FROM creds WHERE name = 'token'")
#     url = "https://notify.eskiz.uz/api/message/sms/get-user-messages"
#     headers = {
#         'Authorization':f'Bearer {token}'
#     }

#     data ={
#         'start_date': '2024-07-30 00:00',
#         'end_date': '2024-07-30 23:59',
#         'page_size': '20',
#         'count' : '0'
#     }

#     res = requests.post(url, headers=headers, data=data)
#     print(res.json())

async def generate_token():
    users_count = await fetch_query("SELECT COUNT(id) FROM test WHERE is_sent = 'false'")
    # Open json file and get text
    with open('extras/sms_text.json', 'r') as file:
        text = json.load(file)

    print(type(text))
    if users_count[0]['count'] > 0:
        users = await fetch_query("SELECT * FROM test WHERE is_sent = 'false'")
        for user in users:
            token = f"https://t.me/dilshodarbot?start={user['phone_number']}_{user['book_id']}"
            res = await make_request(user['name'], user['phone_number'], text, token)
            if res.get('status') == 'success':
                await execute_query(f"UPDATE test SET is_sent = 'true' WHERE id = {user['id']}")
                print(f"Message sent to {user['name']} ({user['phone_number']})")

