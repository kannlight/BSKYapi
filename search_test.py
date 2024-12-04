from atproto import Client
import json
import os
from dotenv import load_dotenv
import datetime
import pickle

inner_data_dir = 'inner_data'

# 認証
load_dotenv()
client = Client()
client.login('kanlight.bsky.social', os.environ.get("pswd"))

# リクエスト
# res = client.search_posts(q='!_exists_:embed AND ')
# res = client.app.bsky.feed.search_posts(params={'q':'-http'})
res = client.app.bsky.feed.search_posts(params={'q':'-http -@', 'lang':'jp', 'limit':100})
# res = client.get_author_feed(actor='did:plc:va3uvvsa2aqfdqvjc44itph4')
# res = client.app.bsky.feed.search_posts(params={'q':'-http -@', 'author':'did:plc:va3uvvsa2aqfdqvjc44itph4'})

json_res = res.model_dump_json()
decoded_res = json.loads(json_res)

# 画像や動画を含む投稿やリプライの親子を持たない投稿を除外
noises = []
for post in decoded_res['posts']:
    if post['embed'] != None:
        noises.append(post)
    elif post['reply_count'] == 0 and (not 'reply' in post['record']) or post['record']['reply'] == None:
        noises.append(post)
    elif post['reply_count'] == 0 and post['record']['reply']['parent'] == post['record']['reply']['root']:
        noises.append(post)
for noise in noises:
    decoded_res['posts'].remove(noise)

# 探索済みの木の集合を読み込み
searched_trees = set()
with open(inner_data_dir+'/searched_trees.txt','rb') as f:
    searched_trees = pickle.load(f)
error_trees = set()
with open(inner_data_dir+'/error_trees.txt','rb') as f:
    error_trees = pickle.load(f)

# デバッグ用出力
print(json.dumps(decoded_res, indent=4, ensure_ascii=False))

for post in decoded_res['posts']:
    # 根を参照
    if post['record']['reply'] != None:
        root_uri = post['record']['reply']['root']['uri']
    else:
        # 自身が根である場合はreplyがnullなので自身のuriを直接参照
        root_uri = post['uri']
    # 木が探索済みでないか確認
    print(root_uri in searched_trees or root_uri in error_trees)

# ファイルに書き出す
# filename = './output_search_test/' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.json'
# with open(filename, 'w') as f:
#     json.dump(decoded_res, f, indent=4)