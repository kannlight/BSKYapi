from atproto import Client
from atproto import exceptions
import json
import os
from dotenv import load_dotenv
import datetime
import pickle
import shutil
import time
import sys

inner_data_dir = 'inner_data' # プログラム内部で使う変数をまとめたフォルダ
output_collect_dir = 'output_collect' # 収集したデータを記録するフォルダ
output_collect_author_dir = 'output_collect_author' # アカウントを指定して収集したデータを記録するフォルダ
creating_data_dir = 'creating_data' # 作成中のデータを溜めておくフォルダ
data_dir = 'data' # 完成したデータを保存するフォルダ
poor_data_dir = 'poor_data' # 対話数が足りないアカウントのデータと判断されたデータを保管しておくフォルダ
logfile = 'log/logfile' # 収集や生成の状況を書き込んでいくログデータのフォルダ
count = 0 # 5分間のリクエスト回数
limit = 3000 # 5分間のリクエスト回数制限
MAX_RETRIES = 5 # インターネットが不安定な場合などリクエストに失敗した時にリトライする回数

# リクエスト制限を超えた状態でリクエスト送信する前に出す例外
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
    # 以下３つの変数はpickleで保存して管理している。これらを初期化するための関数
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
    # データを収集する関数
    global count
    if count >= limit:
        with open(logfile, 'a', encoding='utf-8') as f:
            print('requests reached {} times'.format(count),file=f)
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
    
    # 結果のフィルタリング前に検索結果の時間の範囲を記録
    if len(decoded_res) > 0:
        earliest_time = decoded_res['posts'][0]['record']['created_at']
        oldest_time = decoded_res['posts'][-1]['record']['created_at']
        # jsonに書き込んでおく
        decoded_res['period'] = {'earliest':earliest_time, 'oldest':oldest_time}

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
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(decoded_res, f, indent=4)
    return filename

def create_talk(json_filename):
    # 収集したデータから対話データを生成する関数
    global count
    # ファイルから投稿データを読み込み
    data = {}
    with open(json_filename, 'r', encoding='utf-8') as f:
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
            with open(logfile, 'a', encoding='utf-8') as f:
                print('requests reached {} times'.format(count),file=f)
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
            with open(logfile, 'a', encoding='utf-8') as f:
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

def tree_to_array(tree):
    # 木を根から葉への配列にばらす関数
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
    # 配列として与えられた発話の列から対話データを生成する関数
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
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
        data['data'].append(talk_data)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4,ensure_ascii=False)
        # 探索済みの対話にこの対話を追加
        searched_talks.add(end_uri)
        # 探索済みの対話の集合を保存
        with open(inner_data_dir+'/searched_talks.txt','wb') as f:
            pickle.dump(searched_talks, f)
        # headをずらして繰り返し
        head = next_head

def check_talk(array, head, i):
    # 与えられた配列の中に除外条件に引っかかる発話がないかを判定する関数
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
    # target_fにadder_fの対話データをマージして消去する関数
    target_data = {'data':[]}
    adder_data = {'data':[]}
    with open(target_f, 'r', encoding='utf-8') as f:
        target_data = json.load(f)
    with open(adder_f, 'r', encoding='utf-8') as f:
        adder_data = json.load(f)
    target_data['data'] += adder_data['data']
    with open(target_f, 'w', encoding='utf-8') as f:
        json.dump(target_data, f, indent=4, ensure_ascii=False)
    os.remove(adder_f)

def increase_data(size_TH):
    # 生成した対話データから、size_THを超えるように対話データを増やし、データを完成させる関数
    # 作成中の対話データを順に処理
    for someone_file in os.listdir(creating_data_dir):
        with open(logfile, 'a', encoding='utf-8') as f:
            print('request {} times'.format(count),file=f)
            print(someone_file,file=f)
        # 対話数がsize_THを満たすか確認
        size = size_TH
        with open(creating_data_dir+'/'+someone_file, 'r', encoding='utf-8') as f:
            size = len(json.load(f)['data'])
        last_time = None
        while size < size_TH:
            # 満たない場合は同じアカウントの投稿を新たに収集して対話データを追加生成
            prev_size = size
            filename = collect_data('did:plc:'+someone_file.replace('.json',''), until=last_time)
            create_talk(filename)
            # 対話数を更新
            with open(creating_data_dir+'/'+someone_file, 'r', encoding='utf-8') as f:
                size = len(json.load(f)['data'])
            # 対話数が増えてない場合は対話数が満たないアカウントとみなしてやめる
            if size == prev_size:
                break
            # 毎回同じ対象を検索していると当然対話データは増えないので、期間をずらす
            with open(filename, 'r', encoding='utf-8') as f:
                last_time = json.load(f)['period']['oldest']
        if size < size_TH:
            # 上記のループを経ても対話数が満たなかった場合はpoor_data_dirに保存しておく
            if os.path.exists(poor_data_dir+'/'+someone_file):
                # 既に存在している場合、マージするとsize_THを満たす可能性がある
                merge_data(poor_data_dir+'/'+someone_file, creating_data_dir+'/'+someone_file)
                with open(logfile, 'a', encoding='utf-8') as f:
                    print('  added {} to the poor data'.format(size),file=f)
                # マージ後の対話数をカウント
                poor_size = 0
                with open(poor_data_dir+'/'+someone_file, 'r', encoding='utf-8') as f:
                    poor_size = len(json.load(f))
                if poor_size >= size_TH:
                    # size_THを満たした場合は完成したデータとして追加
                    if os.path.exists(data_dir+'/'+someone_file):
                        # 既に完成したデータに加えられている可能性もある。その場合はマージする。
                        merge_data(data_dir+'/'+someone_file, poor_data_dir+'/'+someone_file)
                        with open(logfile, 'a', encoding='utf-8') as f:
                            print('  the poor data reached {} and merged into the data'.format(poor_size),file=f)
                    else:
                        shutil.move(poor_data_dir+'/'+someone_file, data_dir)
                        with open(logfile, 'a', encoding='utf-8') as f:
                            print('  the poor data reached {}'.format(poor_size),file=f)
            else:
                shutil.move(creating_data_dir+'/'+someone_file, poor_data_dir)
                with open(logfile, 'a', encoding='utf-8') as f:
                    print('  ended in {}'.format(size),file=f)
            continue
        # ループを抜けた結果、対話数がsize_THを満たした場合、完成したデータとして追加する
        shutil.move(creating_data_dir+'/'+someone_file, data_dir)
        with open(logfile, 'a', encoding='utf-8') as f:
            print('  reached {}'.format(size),file=f)
    with open(logfile, 'a', encoding='utf-8') as f:
        print('request {} times'.format(count),file=f)

def test2():
    # デバッグ用、適宜書き換えてmain()の代わりに呼んで使う
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
    # 新たにログファイルを用意
    global logfile
    logfile = 'log/'+datetime.datetime.now().strftime('%Y%m%d_%H%M%S')+'.txt'
    # リクエスト回数をリセット
    global count
    count = 0
    # 最低対話数を設定
    sizeTH = 8
    # 初めて実行する場合は初期化が必要
    if not os.path.exists(inner_data_dir+'/searched_trees.txt'):
        initialize()
    # 最後に収集したデータのファイルを確認
    outputs_collect = os.listdir(output_collect_dir)
    if len(outputs_collect) > 0:
        filename = output_collect_dir+'/'+sorted(outputs_collect)[-1]
    else:
        # 無い場合（初めて実行する場合）は指定なしでデータ収集してくる
        filename = collect_data()
    # 最後のファイルから対話データ生成（前回の続きから作業開始といったイメージ）
    create_talk(filename)
    # 対話数を増やしたりしてデータを完成させる
    increase_data(sizeTH)
    while count < limit:
        # リクエスト制限にかからない間繰り返す
        # 検索期間をずらす
        with open(filename, 'r', encoding='utf-8') as f:
            oldest = json.load(f)['period']['oldest']
        with open(logfile, 'a', encoding='utf-8') as f:
            print('collect until {} '.format(oldest),file=f)
        # 新たに収集、生成、作成
        filename = collect_data(until=oldest)
        create_talk(filename)
        increase_data(sizeTH)

def automate_main(loop_count):
    while loop_count > 0:
        start = time.time()
        try:
            main()
        except ReachedLimit as e:
            pass
        elapsed = time.time() - start
        sleeptime = 360 - elapsed
        if sleeptime > 0:
            time.sleep(sleeptime)
        loop_count -= 1

if __name__ == "__main__":
    if len(sys.argv) > 1:
        automate_main(int(sys.argv[1]))
    else:
        main()