from atproto import Client
import json
import os
from dotenv import load_dotenv
import datetime

# 認証
load_dotenv()
client = Client()
client.login('kanlight.bsky.social', os.environ.get("pswd"))

def collect_data(user_did = 0, since = 0, until = 0):
    res = {}
    if user_did == 0:
        p = {'q':'-http -@', 'lang':'jp', 'limit':100}
        if since != 0:
            p['since'] = since
        if until != 0:
            p['until'] = until
        res = client.app.bsky.feed.search_posts(params=p)
    else:
        res = client.get_author_feed(actor=user_did, limit=100)
    decoded_res = json.loads(res.model_dump_json())
    noises = []
    if user_did == 0:
        for post in decoded_res['posts']:
            if post['embed'] != None:
                noises.append(post)
            elif post['reply_count'] == 0 and post['record']['reply'] == None:
                noises.append(post)
        for noise in noises:
            decoded_res['posts'].remove(noise)
    else:
        for post in decoded_res['feed']:
            if post['post']['embed'] != None:
                noises.append(post)
            elif post['post']['reply_count'] == 0 and post['post']['record']['reply'] == None:
                noises.append(post)
        for noise in noises:
            decoded_res['feed'].remove(noise)
    # ファイルに書き出す
    filename = './output_collect_test/' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.json'
    with open(filename, 'w') as f:
        json.dump(decoded_res, f, indent=4)
    
def test():
    # collect_data()
    collect_data(user_did='did:plc:ruwnuvzigdl3527oe3vjrqwj')

if __name__ == "__main__":
    test()