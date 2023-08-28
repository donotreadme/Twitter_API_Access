import mysql.connector
from mysql.connector import Error
from datetime import datetime
import logging

def connect_to_database(connection_data):
    """ Connect to MySQL database """
    conn = None
    try:
        conn = mysql.connector.connect(host=connection_data["host"],
                                       database=connection_data["database"],
                                       user=connection_data["user"],
                                       password=connection_data["password"])
        if conn.is_connected():
            logging.info('Connected to MySQL database')
    except Error as e:
        logging.error(e)
    return conn

def insert_tweet_into_database(keyword, response_data_row, database_connection):
    mycursor = database_connection.cursor()
    sql = '''INSERT INTO tweets (keyword, tweet_id, author_id, text, created_at, in_reply_to_user_id, conversation_id, 
    retweet_count, reply_count, like_count, quote_count, impression_count, reply_settings) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s)'''
    created_at = datetime.fromisoformat(response_data_row['created_at'].replace('Z', ''))
    val = (keyword, response_data_row.get('id'), response_data_row.get('author_id'), response_data_row.get('text'), created_at, \
        response_data_row.get('in_reply_to_user_id'), response_data_row.get('conversation_id'), response_data_row['public_metrics'].get('retweet_count'), \
        response_data_row['public_metrics'].get('reply_count'), response_data_row['public_metrics'].get('like_count'), \
        response_data_row['public_metrics'].get('quote_count'), response_data_row['public_metrics'].get('impression_count'), response_data_row.get('reply_settings')) 
    mycursor.execute(sql, val)
    database_connection.commit()
    mycursor.close()
    pass

def check_if_tweets_is_in_database(id, database_connection):
    mycursor = database_connection.cursor()
    query = "SELECT id FROM tweets WHERE tweet_id=" + str(id)
    mycursor.execute(query)
    if len(mycursor.fetchall()) > 0:
        return True
    else:
        return False

def insert_user_into_database(user_response, database_connection):
    mycursor = database_connection.cursor()
    query = '''INSERT INTO users 
    (user_id, username, name, created_at, profile_description, verified, protected, 
    location, tweet_count, following_count, followers_count) VALUES 
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    created_at = datetime.fromisoformat(user_response.get('created_at').replace('Z', ''))
    val = (user_response.get('id'), user_response.get('username'), user_response.get('name'), created_at, \
        user_response.get('description'), user_response.get('verified'), user_response.get('protected'), \
        user_response.get('location'), user_response.get('public_metrics').get('tweet_count'), \
        user_response.get('public_metrics').get('following_count'), user_response.get('public_metrics').get('followers_count'))
    mycursor.execute(query, val)
    database_connection.commit()
    mycursor.close()
    pass

def insert_following(user_id, response_data, database_connection):
    mycursor = database_connection.cursor()
    query = "INSERT IGNORE INTO followers (user_id, is_following) VALUES (%s, %s)"
    data = []
    for row in response_data:
        data.append((user_id, row['id']))
    mycursor.executemany(query, data)
    database_connection.commit()

def insert_followers(user_id, response_data, database_connection):
    mycursor = database_connection.cursor()
    query = "INSERT IGNORE INTO followers (user_id, is_following) VALUES (%s, %s)"
    data = []
    for row in response_data:
        data.append((row['id'], user_id))
    mycursor.executemany(query, data)
    database_connection.commit()


def check_if_user_is_in_database(id, database_connection):
    mycursor = database_connection.cursor()
    query = "SELECT id FROM users WHERE user_id=" + str(id)
    mycursor.execute(query)
    if len(mycursor.fetchall()) > 0:
        return True
    else:
        return False

def get_sample_of_recent_tweets(database_connection):
    # TODO: write a method under usage of several parameters. Namly: limit of the time range and rank of user importance
    # Also check if user is already in user database 
    pass
