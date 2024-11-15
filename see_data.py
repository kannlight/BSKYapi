import json
import sys

args = sys.argv

for i in range(1,len(args)):
    with open(args[i], 'r') as f:
        data = json.load(f)
        print(args[i])
        print(json.dumps(data, indent=4, ensure_ascii=False))
