from atproto import Client
import json
import os
from dotenv import load_dotenv
import datetime
import pickle

# 認証
load_dotenv()
client = Client()
client.login('kanlight.bsky.social', os.environ.get("pswd"))

def initialize(inner_data_dir = 'inner_data'):
    searched_trees = set()
    with open(inner_data_dir+'/searched_trees.txt','xb') as f:
        pickle.dump(searched_trees, f)
    searched_talks = set()
    with open(inner_data_dir+'/searched_talks.txt','xb') as f:
        pickle.dump(searched_talks, f)

def collect_data(user_did = None, since = None, until = None, output_collect_dir = 'output_collect'):
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
        elif post['reply_count'] == 0 and post['record']['reply']['parent'] == post['record']['reply']['root']:
            noises.append(post)
    for noise in noises:
        decoded_res['posts'].remove(noise)
    
    # ファイルに書き出す
    filename = output_collect_dir+'/'+datetime.datetime.now().strftime('%Y%m%d_%H%M%S')+'.json'
    with open(filename, 'w') as f:
        json.dump(decoded_res, f, indent=4)
    return filename

def create_talk(json_filename, inner_data_dir = 'inner_data', data_dir = 'data'):
    # ファイルから投稿データを読み込み
    data = {}
    with open(json_filename, 'r') as f:
        data = json.load(f)
    # 探索済みの木の集合を読み込み
    searched_trees = set()
    with open(inner_data_dir+'/searched_trees.txt','rb') as f:
        searched_trees = pickle.load(f)
    # 各投稿について処理
    for post in data['posts']:
        # 根を参照
        root_uri = post['record']['reply']['root']['uri']
        # 木が探索済みでないか確認
        if root_uri in searched_trees:
            continue
        # 木全体を取得
        res = client.get_post_thread(uri=root_uri, depth=1000)
        thread = res.thread.model_dump_json()
        decoded_thread = json.loads(thread)

        # 木を配列にばらす
        utters_set = tree_to_array(decoded_thread)
        # 各配列から対話データを抽出
        for utters in utters_set:
            extract_talk_from_array(utters, inner_data_dir, data_dir)
        
        # 探索済みの木にこの木を追加
        searched_trees.add(root_uri)
        # 探索済みの木の集合を保存
        with open(inner_data_dir+'/searched_trees.txt','wb') as f:
            pickle.dump(searched_trees, f)

# 木を根から葉への配列にばらす関数。
def tree_to_array(tree):
    array_set = []
    # 子ノードがあるか
    if 'replies' in tree and len(tree['replies']) != 0:
        # 各子ノードについて処理
        for reply in tree['replies']:
            # 各子ノードを根とする木から出来た配列をまとめる
            array_set.extend(tree_to_array(reply))
        # 各配列に対して先頭に自身を追加
        for array in array_set:
            array.insert(0, tree['post'])
    # 子ノードがないなら
    else:
        # 自身(葉)だけを含むリストを作って追加
        array_set.append([tree['post']])
    return array_set

def extract_talk_from_array(array, inner_data_dir, data_dir):
    # 探索済みの対話の集合を読み込み
    searched_talks = set()
    with open(inner_data_dir+'/searched_talks.txt','rb') as f:
        searched_talks = pickle.load(f)
    l = len(array)
    # 末尾から3発話目までを処理、先頭をheadとする
    head = 0
    while head < l-2:
        i = head

        if check_utter(array[i]):
            break
        # 受信者(暫定)の最後の発話
        recept_did = array[i]['author']['did']
        last_utter = array[i]['record']['text']
        i += 1
        while i < l and array[i]['author']['did'] == recept_did:
            if check_utter(array[i]):
                break
            # 同じ投稿者の投稿が続く場合はスペースで結合することにする(特に理由はない)
            last_utter += ' ' + array[i]['record']['text']
            i += 1
        if i >= l:
            break
        # 次のheadの位置を記憶しておく
        next_head = i

        if check_utter(array[i]):
            break
        # 送信者の発話
        sender_did = array[i]['author']['did']
        sent_utter = array[i]['record']['text']
        i += 1
        if i >= l:
            break
        while i < l and array[i]['author']['did'] == sender_did:
            if check_utter(array[i]):
                break
            # 同じ投稿者の投稿が続くならスペースで結合
            sent_utter += ' ' + array[i]['record']['text']
            i += 1
        if i >= l:
            break

        if check_utter(array[i]):
            break
        # 1発話目と3発話目が同じ人でなければ対話データ不成立
        if array[i]['author']['did'] != recept_did:
            head = next_head
            continue

        # 受信者の次の発話
        forecast_utter = array[i]['record']['text']

        # 対話が探索済みでないか確認
        end_uri = array[i]['uri']
        if end_uri in searched_talks:
            head = next_head
            continue

        i += 1
        while i < l and array[i]['author']['did'] == recept_did:
            if check_utter(array[i]):
                break
            # 同じ投稿者の投稿が続くならスペースで結合
            forecast_utter += ' ' + array[i]['record']['text']
            i += 1

        # 対話データ完成
        talk = {'last_utter':last_utter, 'sent_utter':sent_utter, 'forecast_utter':forecast_utter}
        # 完成したデータを書き込む
        filename = data_dir+'/'+recept_did+'.json'
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)
        else:
            data = {'data':[]}
        data['data'].append(talk)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)

        # 探索済みの対話にこの対話を追加
        searched_talks.add(end_uri)
        # 探索済みの対話の集合を保存
        with open(inner_data_dir+'/searched_talks.txt','wb') as f:
            pickle.dump(searched_talks, f)

        # headをずらして繰り返し
        head = next_head

def check_utter(post):
    # 除外条件：リンクやメンション、画像、動画を含む投稿
    return (not 'http' in post['record']['text']) and (not '@' in post['record']['text']) and post['embed'] == None

def test():
    output_collect_dir = 'output_collect_test'
    data_dir = 'data_test'
    inner_data_dir = 'inner_data_test'
    # collect_data(None, None, None, output_collect_dir)
    # collect_data(user_did='did:plc:va3uvvsa2aqfdqvjc44itph4')
    # initialize(inner_data_dir)
    create_talk('output_collect_test/20241112_122652.json', inner_data_dir, data_dir)

if __name__ == "__main__":
    test()