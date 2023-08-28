import database_access
import twitter_access
import json
import time
import logging

# basic setup, gets twitter and database connection
logging.basicConfig(format='%(asctime)s %(levelname)s - %(message)s', level=logging.INFO, filename="get_users.log")
with open("config.json") as f:
        config = json.load(f)
database_connection = database_access.connect_to_database(config['database'])
session = twitter_access.get_twitter_session(config['bearer_token'])

# get a list of all unique users in the tweets database
mycursor = database_connection.cursor()
query = "SELECT DISTINCT author_id FROM tweets"
#query = "SELECT DISTINCT author_id FROM day_one WHERE NOT EXISTS(SELECT * FROM users WHERE day_one.author_id = users.user_id)"
mycursor.execute(query)
results = mycursor.fetchall()
logging.info("{} results received from database".format(len(results)))
counter = 0
for result in results: #result is a tuble with only one element
    try: # TODO check if user is already in database before adding them
        try_again = True
        while(try_again):       
            try_again = False
            user_data = None
            try:
                # try to get the user from the twitter api, throws exception if response is not valid
                user = twitter_access.get_infos_by_userId(result[0], session)
                user_data = user['data']
            except twitter_access.UserDoesNotExistException as e:
                with open("deleted.txt", "a") as f:
                    f.write(e.message + "\n")
                logging.debug('Deleted user written into external file')
            except twitter_access.UserIsSuspended as e:
                with open("suspended.txt", "a") as f:
                    f.write(e.message + "\n")
                logging.debug('Suspended user written into external file')
            except twitter_access.TooManyRequestException:
                # api limit reached, wait for 15 minutes and repeat loop afterwards
                try_again = True
                minutes = 15
                logging.info("Inserted {} new users and is now waiting {} minutes to reset the api limit".format(counter, minutes))
                counter = 0
                time.sleep(minutes * 60)
            except Exception as e:
                logging.warning("Inner exception occurred: {}".format(e))
        if user_data != None:
            # if responed was valid save it into the database
            database_access.insert_user_into_database(user_data, database_connection)
            counter += 1
    except:
        logging.error("Outer exception occurred", exc_info=True)
logging.info("End of script reached")
