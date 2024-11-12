from atproto import Client
import json
import os
from dotenv import load_dotenv
import datetime

# 認証
load_dotenv()
client = Client()
client.login('kanlight.bsky.social', os.environ.get("pswd"))

def collect_data(user_did = None, since = None, until = None):
    # 検索クエリを設定
    p = {'q':'-http -@', 'lang':'ja', 'limit':100}
    if user_did != None:
        p['author'] = user_did
    if since != None:
        p['since'] = since
    if until != None:
        p['until'] = until
    # デバッグ用
    print(p)
    # 検索結果を取得しjsonへ変換
    res = client.app.bsky.feed.search_posts(params=p)
    decoded_res = json.loads(res.model_dump_json())

    # 画像や動画を含む投稿やリプライの親子を持たない投稿を除外
    noises = []
    for post in decoded_res['posts']:
        if post['embed'] != None:
            noises.append(post)
        elif post['reply_count'] == 0 and post['record']['reply'] == None:
            noises.append(post)
    for noise in noises:
        decoded_res['posts'].remove(noise)
    
    # ファイルに書き出す
    filename = './output_collect_test/' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.json'
    with open(filename, 'w') as f:
        json.dump(decoded_res, f, indent=4)
    
def test():
    collect_data()
    # collect_data(user_did='did:plc:va3uvvsa2aqfdqvjc44itph4')

if __name__ == "__main__":
    test()