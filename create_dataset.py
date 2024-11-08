from atproto import Client
import json
import os
from dotenv import load_dotenv
import datetime

# 認証
load_dotenv()
client = Client()
client.login('kanlight.bsky.social', os.environ.get("pswd"))

# リクエスト
res = client.get_post_thread(uri='at://did:plc:qatx2fvwppss5d3qye6tpvcu/app.bsky.feed.post/3l7oysmcxqu2q')
thread = res.thread.model_dump_json()
decoded_thread = json.loads(thread)

# デバッグ用出力
# print(json.dumps(decoded_thread, indent=4, sort_keys=True, ensure_ascii=False))

# ファイルに書き出す
filename = './output_test/' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.json'
with open(filename, 'w') as f:
    json.dump(decoded_thread, f, indent=4)

def collect_data(user_name = 0):
    query = 
    client.search_posts(query)