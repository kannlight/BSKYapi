from atproto import Client
import json
import os
from dotenv import load_dotenv
import datetime
from atproto import exceptions

# 認証
load_dotenv()
client = Client()
client.login('kanlight.bsky.social', os.environ.get("pswd"))

# リクエスト
try:
    res = client.get_post_thread(uri='at://did:plc:aaytn2edne7zd3izb5dma3kb/app.bsky.feed.post/3kr43ddzvhw2h')
    thread = res.thread.model_dump_json()
    decoded_thread = json.loads(thread)
    # デバッグ用出力
    print(json.dumps(decoded_thread, indent=4, sort_keys=True, ensure_ascii=False))
except exceptions.BadRequestError as e:
    print(e)


# ファイルに書き出す
# filename = './output_test/' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.json'
# with open(filename, 'w') as f:
#     json.dump(decoded_thread, f, indent=4)