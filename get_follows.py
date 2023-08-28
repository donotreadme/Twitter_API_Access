import database_access
import twitter_access
import json
import time
import logging
from datetime import datetime

# basic setup, get twitter and database connection
logging.basicConfig(format='%(asctime)s %(levelname)s - %(message)s', level=logging.INFO, filename="get_follows.log")
with open("config.json") as f:
        config = json.load(f)
database_connection = database_access.connect_to_database(config['database']) # TODO move this to the database_access class
session = twitter_access.get_twitter_session(config['bearer_token'])

# get a list of all unique entries in the users database
mycursor = database_connection.cursor()
query = "SELECT DISTINCT user_id, protected FROM users" # FIXME changes this after the initial crawl (needs to check if the user has posted a tweet)
mycursor.execute(query)
results = mycursor.fetchall()
logging.info("{} results received from database".format(len(results)))
counter = 0
timestamps = []
for result in results: #result is a tuble with two element: user_id and protected (0 as false)
    try: 
        try_again = True
        while(try_again):       
            try_again = False
            following = None
            followers = None
            # sort the results out where we have no access on the followers
            if result[1] == 0:
                try:
                    following = twitter_access.get_following_by_userId(result[0], session)['data']
                    followers = twitter_access.get_followers_by_userId(result[0], session)['data']                
                except twitter_access.TooManyRequestException:
                    # if api limit reached, get how much time has passed since the last 15 requests and calculate how much time it has to wait for the full 15 minutes
                    try_again = True
                    if len(timestamps) >= 15:
                        secs = 15 * 60 - (timestamps[-1] - timestamps[-15]).total_seconds()
                    else:
                        secs = 15 * 60
                    logging.info("Inserted {} new follows and is now waiting {} seconds to reset the api limit".format(counter, secs))
                    counter = 0
                    time.sleep(secs)
                except KeyError as e:
                    logging.warning("KeyError from UserId {}, probably it no longer exists or has no followers / following".format(result[0]))
                except Exception as e:
                    logging.warning("Inner exception occurred: {}".format(e))        
        # if responed was valid save it into the database         
        timestamps.append(datetime.now())       
        if following != None:
            database_access.insert_following(result[0], following, database_connection)
            for user in following:
                database_access.insert_user_into_database(user, database_connection)
            counter += 1
        if followers != None:
            database_access.insert_followers(result[0], followers, database_connection)
            for user in followers:
                database_access.insert_user_into_database(user, database_connection)
            counter += 1
    except:
        logging.error("Outer exception occurred", exc_info=True)
logging.info("End of script reached")
