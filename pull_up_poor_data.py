import json
import os
import shutil

data_dir = 'data'
poor_data_dir = 'poor_data'

def main(size_TH):
    for file in os.listdir(poor_data_dir):
        with open(poor_data_dir+'/'+file, 'r', encoding='utf-8') as f:
            size = len(json.load(f)['data'])
        if size >= size_TH:
            if os.path.exists(data_dir+'/'+file):
                # 既に完成したデータに加えられている可能性もある。その場合はマージする。
                merge_data(data_dir+'/'+file, poor_data_dir+'/'+file)
            else:
                shutil.move(poor_data_dir+'/'+file, data_dir)

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

if __name__ == "__main__":
    size_TH = 8
    main(size_TH)