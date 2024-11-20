from atproto import Client
import json
import os
from dotenv import load_dotenv
import datetime
import sys

# 認証
load_dotenv()
client = Client()
client.login('kanlight.bsky.social', os.environ.get("pswd"))

# リクエスト
args = sys.argv
res = client.get_posts(uris=[args[1]])
json_res = res.model_dump_json()
decoded_res = json.loads(json_res)

# デバッグ用出力
print(json.dumps(decoded_res, indent=4, sort_keys=True, ensure_ascii=False))

# ファイルに書き出す
# filename = './output_test/' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.json'
# with open(filename, 'w') as f:
#     json.dump(decoded_res, f, indent=4)