import requests
import json

def get_twitter_session(bearer_token):
    session = requests.Session()
    session.headers.update({'Authorization': f"Bearer {bearer_token}"})
    return session

def get_search_results_for_keyword(session, keyword, language = "en", next_token = None, allowReply=False):
    url = "https://api.twitter.com/2/tweets/search/recent?query={} -is:retweet -is:reply lang:{}&max_results=100&tweet.fields=author_id,public_metrics,created_at,reply_settings,conversation_id,in_reply_to_user_id"\
        .format(keyword, language)
    if allowReply:
        url = url.replace("-is:reply ", "")
    if next_token != None:
        url = url + "&next_token=" + next_token
    response = session.get(url) 
    response = response.json()
    exception_handling(response)
    return response

def get_following_by_userId(userId, session):
    url = "https://api.twitter.com/2/users/:id/following?user.fields=created_at,description,id,location,name,protected,public_metrics,username,verified&max_results=1000"\
        .replace(":id", str(userId))
    response = session.get(url)
    response = response.json()
    exception_handling(response, userId)
    return response

def get_following_id_by_userId(userId, session):
    url = "https://api.twitter.com/2/users/:id/following?user.fields=id&max_results=1000"\
        .replace(":id", str(userId))
    response = session.get(url)
    response = response.json()
    exception_handling(response, userId)
    return response

def get_followers_by_userId(userId, session):
    url = "https://api.twitter.com/2/users/:id/followers?user.fields=created_at,description,id,location,name,protected,public_metrics,username,verified&max_results=1000"\
        .replace(":id", str(userId))
    response = session.get(url)
    response = response.json()
    exception_handling(response, userId)
    return response

def get_followers_id_by_userId(userId, session):
    url = "https://api.twitter.com/2/users/:id/followers?user.fields=id&max_results=1000"\
        .replace(":id", str(userId))
    response = session.get(url)
    response = response.json()
    exception_handling(response, userId)
    return response

def get_infos_by_userId(userId, session):
    url = "https://api.twitter.com/2/users/:id?user.fields=created_at,description,id,location,name,protected,public_metrics,username,verified"\
        .replace(":id", str(userId))
    response = session.get(url)
    response = response.json()
    exception_handling(response, userId)
    return response

def exception_handling(response_json, user_id = 0):
    if 'errors' in response_json.keys():
        if response_json['errors'][0]['title'] == "Not Found Error":
            raise UserDoesNotExistException(response_json['errors'][0]['value'])
        elif "User has been suspended" in response_json['errors'][0]['detail']:
            raise UserIsSuspended(response_json['errors'][0]['value'])
        elif response_json['errors'][0]['title'] == "Authorization Error":
            raise NotAuthorizedException(response_json['errors'][0]['value'])
    elif 'title' in response_json.keys() and response_json['title'] == 'Too Many Requests':
        raise TooManyRequestException(user_id)

class UserDoesNotExistException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class UserIsSuspended(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class NotAuthorizedException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class TooManyRequestException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
