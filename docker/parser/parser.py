from telethon.sync import TelegramClient, events
from telethon.sessions import StringSession
import os
import json
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd

def make_connection():

    postgres_dic = {}

    postgres_dic['dbname'] = os.environ.get('POSTGRES_DB')
    postgres_dic['user'] = os.environ.get('POSTGRES_USER')
    postgres_dic['port'] = 5432
    postgres_dic['host'] = 'postgres'

    with open(os.environ.get('POSTGRES_PASSWORD_FILE'), 'r') as f:
        postgres_dic['password'] = f.readlines()[0]

    return create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
                postgres_dic['user'], postgres_dic['password'], 
                postgres_dic['host'], postgres_dic['port'], postgres_dic['dbname']), pool_pre_ping=True)


def make_client():

    with open(os.environ.get('TELEGRAM_SECRETS_FILE'), 'r') as f:
        dic = json.load(f)

    SESSION = dic['SESSION']
    API_ID = dic['API_ID']
    API_HASH = dic['API_HASH']

    return TelegramClient(StringSession(SESSION), API_ID, API_HASH)

async def upload_data(conn, event, chat, table_name):

    news_dic = dict()
    news_dic['news_text'] = [event.text]
    news_dic['news_source'] = [chat.username]
    news_dic['date_collected'] = [datetime.now().isoformat(sep=' ', timespec='seconds')]
    news_dic['news_date'] = [event.date.isoformat(sep=' ', timespec='seconds')]
    
    df = pd.DataFrame().from_dict(news_dic)

    df.to_sql(table_name, conn, index=False, if_exists='append', method='multi')

def main():

    conn = make_connection()
    table_name = 'news'
    #РИА новости, ТАСС
    chats = [-1001101170442, -1001050820672]

    client = make_client()
    client.start()
    

    @client.on(events.NewMessage(chats = chats))
    async def handler(event):
        
        chat  = await event.get_chat()
        await upload_data(conn, event, chat, table_name)

    client.run_until_disconnected()
   

if __name__ == '__main__':
    main()


