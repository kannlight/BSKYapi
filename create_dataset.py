from atproto import Client
from atproto import exceptions
import json
import os
from dotenv import load_dotenv
import datetime
import pickle
import shutil
import time

inner_data_dir = 'inner_data'
output_collect_dir = 'output_collect'
output_collect_author_dir = 'output_collect_author'
creating_data_dir = 'creating_data'
data_dir = 'data'
poor_data_dir = 'poor_data'
logfile = 'log/logfile'
count = 0
limit = 3000
MAX_RETRIES = 5

class ReachedLimit(Exception):
    pass

# 認証
load_dotenv()
for attempt in range(MAX_RETRIES):
    try:
        client = Client()
        client.login('kanlight.bsky.social', os.environ.get("pswd"))
        break  # 成功した場合、ループを抜ける
    except (exceptions.InvokeTimeoutError,exceptions.NetworkError) as e:
        if attempt < MAX_RETRIES - 1:
            time.sleep(2 ** attempt)  # リトライ前に指数的に待機
        else:
            raise e  # リトライ上限に達したら例外をスロー

def initialize():
    searched_trees = set()
    with open(inner_data_dir+'/searched_trees.txt','xb') as f:
        pickle.dump(searched_trees, f)
    error_trees = set()
    with open(inner_data_dir+'/error_trees.txt','xb') as f:
        pickle.dump(error_trees, f)
    searched_talks = set()
    with open(inner_data_dir+'/searched_talks.txt','xb') as f:
        pickle.dump(searched_talks, f)

def collect_data(user_did = None, since = None, until = None):
    global count
    if count >= limit:
        raise ReachedLimit('リクエスト制限到達')
    # 検索クエリを設定
    p = {'q':'-http -@', 'lang':'ja', 'limit':100}
    if user_did != None:
        p['author'] = user_did
    if since != None:
        p['since'] = since
    if until != None:
        p['until'] = until
    # 検索結果を取得しjsonへ変換
    for attempt in range(MAX_RETRIES):
        try:
            count += 1
            res = client.app.bsky.feed.search_posts(params=p)
            break  # 成功した場合、ループを抜ける
        except (exceptions.InvokeTimeoutError,exceptions.NetworkError) as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)  # リトライ前に指数的に待機
            else:
                raise e  # リトライ上限に達したら例外をスロー
    decoded_res = json.loads(res.model_dump_json())
    
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
    
    # ファイルに書き出す
    if user_did == None:
        filename = output_collect_dir+'/'+datetime.datetime.now().strftime('%Y%m%d_%H%M%S')+'.json'
    else:
        filename = output_collect_author_dir+'/'+datetime.datetime.now().strftime('%Y%m%d_%H%M%S')+'.json'
    with open(filename, 'w') as f:
        json.dump(decoded_res, f, indent=4)
    return filename

def create_talk(json_filename):
    global count
    # ファイルから投稿データを読み込み
    data = {}
    with open(json_filename, 'r') as f:
        data = json.load(f)
    # 探索済みの木の集合を読み込み
    searched_trees = set()
    with open(inner_data_dir+'/searched_trees.txt','rb') as f:
        searched_trees = pickle.load(f)
    error_trees = set()
    with open(inner_data_dir+'/error_trees.txt','rb') as f:
        error_trees = pickle.load(f)
    # 各投稿について処理
    for post in data['posts']:
        if count >= limit:
            raise ReachedLimit('リクエスト制限到達')
        # 根を参照
        if post['record']['reply'] != None:
            root_uri = post['record']['reply']['root']['uri']
        else:
            # 自身が根である場合はreplyがnullなので自身のuriを直接参照
            root_uri = post['uri']
        # 木が探索済みでないか確認
        if root_uri in searched_trees or root_uri in error_trees:
            continue
        # 木全体を取得
        try:
            for attempt in range(MAX_RETRIES):
                try:
                    count += 1
                    res = client.get_post_thread(uri=root_uri, depth=1000)
                    break  # 成功した場合、ループを抜ける
                except (exceptions.InvokeTimeoutError,exceptions.NetworkError) as e:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)  # リトライ前に指数的に待機
                    else:
                        raise e  # リトライ上限に達したら例外をスロー
        except exceptions.BadRequestError:
            # 投稿が削除されている場合など何かしらエラーが返ってきたらスキップ
            with open(logfile, 'a') as f:
                print('cause error in tree {}'.format(root_uri),file=f)
            error_trees.add(root_uri)
            with open(inner_data_dir+'/error_trees.txt','wb') as f:
                pickle.dump(error_trees, f)
            continue
        thread = res.thread.model_dump_json()
        decoded_thread = json.loads(thread)

        # 木を配列にばらす
        utters_set = tree_to_array(decoded_thread)
        # 各配列から対話データを抽出
        for utters in utters_set:
            extract_talk_from_array(utters)
        
        # 探索済みの木にこの木を追加
        searched_trees.add(root_uri)
        # 探索済みの木の集合を保存
        with open(inner_data_dir+'/searched_trees.txt','wb') as f:
            pickle.dump(searched_trees, f)

# 木を根から葉への配列にばらす関数。
def tree_to_array(tree):
    array_set = []
    # 子ノードがあるか
    if tree['replies'] != None:
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

def extract_talk_from_array(array):
    # 探索済みの対話の集合を読み込み
    searched_talks = set()
    with open(inner_data_dir+'/searched_talks.txt','rb') as f:
        searched_talks = pickle.load(f)
    # 作成済みの受信者の集合を読み込み
    created_files = os.listdir(data_dir)
    l = len(array)
    head = 0
    # 末尾から3発話目までを処理、先頭をheadとする
    while head < l-2:
        talk = []
        i = head
        # 受信者(暫定)の最後の発話
        recept_did = array[i]['author']['did']
        while i < l and array[i]['author']['did'] == recept_did:
            talk.append({'author':recept_did, 'type':1, 'utter':array[i]['record']['text']})
            i += 1
        if i >= l:
            break
        # 次のheadの位置を記憶しておく
        next_head = i
        # 作成済みの受信者ならスキップ
        if recept_did.replace('did:plc:', '')+'.json' in created_files:
            head = next_head
            continue
        # 送信者の発話
        sender_did = array[i]['author']['did']
        while i < l and array[i]['author']['did'] == sender_did:
            talk.append({'author':sender_did, 'type':2, 'utter':array[i]['record']['text']})
            i += 1
        if i >= l:
            break
        # 1発話目と3発話目が同じ人でなければ対話データ不成立
        if array[i]['author']['did'] != recept_did:
            head = next_head
            continue
        # 対話が探索済みでないか確認
        end_uri = array[i]['uri']
        if end_uri in searched_talks:
            head = next_head
            continue
        # 受信者の次の発話
        while i < l and array[i]['author']['did'] == recept_did:
            talk.append({'author':recept_did, 'type':3, 'utter':array[i]['record']['text']})
            i += 1
        # 除外条件：リンクやメンション、画像、動画を含む投稿 を満たしてないか確認
        if check_talk(array, head, i):
            head = next_head
            continue
        # 対話データ完成
        talk_data = {'talk':talk, 'uri':end_uri}
        # 完成したデータを書き込む
        filename = creating_data_dir+'/'+recept_did.replace('did:plc:', '')+'.json'
        data = {'data':[]}
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)
        data['data'].append(talk_data)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4,ensure_ascii=False)
        # 探索済みの対話にこの対話を追加
        searched_talks.add(end_uri)
        # 探索済みの対話の集合を保存
        with open(inner_data_dir+'/searched_talks.txt','wb') as f:
            pickle.dump(searched_talks, f)
        # headをずらして繰り返し
        head = next_head

def check_talk(array, head, i):
    for j in range(head,i):
        # 除外条件：リンクやメンション、画像、動画を含む投稿
        if 'http' in array[j]['record']['text'] or '@' in array[j]['record']['text'] or array[j]['embed'] != None:
            return True
    return False

# def test():
#     count = 0
#     output_collect_dir = 'output_collect_test'
#     data_dir = 'data_test'
#     inner_data_dir = 'inner_data_test'
#     # collect_data(None, None, None, output_collect_dir)
#     # collect_data(user_did='did:plc:va3uvvsa2aqfdqvjc44itph4')
#     # initialize(inner_data_dir)
#     count = create_talk('output_collect_test/20241112_122652.json', count, inner_data_dir, data_dir)
#     print(count)  

def merge_data(target_f,adder_f):
    # 完成したデータを書き込む
        target_data = {'data':[]}
        adder_data = {'data':[]}
        with open(target_f, 'r') as f:
            target_data = json.load(f)
        with open(adder_f, 'r') as f:
            adder_data = json.load(f)
        target_data['data'] += adder_data['data']
        with open(target_f, 'w') as f:
            json.dump(target_data, f, indent=4, ensure_ascii=False)
        os.remove(adder_f)

def increase_data(size_TH):
    for someone_file in os.listdir(creating_data_dir):
        with open(logfile, 'a') as f:
            print('request {} times'.format(count),file=f)
            print(someone_file,file=f)
        size = size_TH
        with open(creating_data_dir+'/'+someone_file, 'r') as f:
            size = len(json.load(f)['data'])
        last_time = None
        while size < size_TH:
            prev_size = size
            filename = collect_data('did:plc:'+someone_file.replace('.json',''), until=last_time)
            create_talk(filename)
            with open(creating_data_dir+'/'+someone_file, 'r') as f:
                size = len(json.load(f)['data'])
            if size == prev_size:
                break
            with open(filename, 'r') as f:
                last_time = json.load(f)['posts'][-1]['record']['created_at']
        if size < size_TH:
            if os.path.exists(poor_data_dir+'/'+someone_file):
                merge_data(poor_data_dir+'/'+someone_file, creating_data_dir+'/'+someone_file)
                with open(logfile, 'a') as f:
                    print('  added {} to the poor data'.format(size),file=f)
                poor_size = 0
                with open(poor_data_dir+'/'+someone_file, 'r') as f:
                    poor_size = len(json.load(f))
                if poor_size >= size_TH:
                    if os.path.exists(data_dir+'/'+someone_file):
                        merge_data(data_dir+'/'+someone_file, poor_data_dir+'/'+someone_file)
                        with open(logfile, 'a') as f:
                            print('  the poor data reached {} and merged into the data'.format(poor_size),file=f)
                    else:
                        shutil.move(poor_data_dir+'/'+someone_file, data_dir)
                        with open(logfile, 'a') as f:
                            print('  the poor data reached {}'.format(poor_size),file=f)
            else:
                shutil.move(creating_data_dir+'/'+someone_file, poor_data_dir)
                with open(logfile, 'a') as f:
                    print('  ended in {}'.format(size),file=f)
            continue
        shutil.move(creating_data_dir+'/'+someone_file, data_dir)
        with open(logfile, 'a') as f:
            print('  reached {}'.format(size),file=f)
    with open(logfile, 'a') as f:
        print('request {} times'.format(count),file=f)

def test2():
    global output_collect_dir, creating_data_dir, data_dir, poor_data_dir, inner_data_dir
    output_collect_dir = 'test_data/output_collect_test2-9'
    creating_data_dir = 'test_data/creating_data_test2-9'
    data_dir = 'test_data/data_test2-9'
    poor_data_dir = 'test_data/poor_data_test2-9'
    inner_data_dir = 'test_data/inner_data_test2-9'
    sizeTH = 10
    # initialize()
    filename = collect_data()
    create_talk(filename)
    increase_data(sizeTH)
    
def main():
    global logfile
    logfile = 'log/'+datetime.datetime.now().strftime('%Y%m%d_%H%M%S')+'.txt'
    count = 0
    sizeTH = 10
    if not os.path.exists(inner_data_dir+'/searched_trees.txt'):
        initialize()
    outputs_collect = os.listdir(output_collect_dir)
    if len(outputs_collect) > 0:
        filename = output_collect_dir+'/'+sorted(outputs_collect)[-1]
    else:
        filename = collect_data()
    create_talk(filename)
    increase_data(sizeTH)
    while count < limit:
        with open(filename, 'r') as f:
            oldest = json.load(f)['posts'][-1]['record']['created_at']
        with open(logfile, 'a') as f:
            print('collect until {} '.format(oldest),file=f)
        filename = collect_data(until=oldest)
        create_talk(filename)
        increase_data(sizeTH)

if __name__ == "__main__":
    main()