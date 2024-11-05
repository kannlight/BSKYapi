from atproto import Client
import json
import os
from dotenv import load_dotenv

load_dotenv()

client = Client()
client.login('kanlight.bsky.social', os.environ.get("pswd"))

res = client.get_post_thread(uri='at://did:plc:qatx2fvwppss5d3qye6tpvcu/app.bsky.feed.post/3l7oysmcxqu2q')
thread = res.thread.json()

json.dumps(thread)