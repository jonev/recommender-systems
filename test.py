import os
import json
import project_example as pe
import logging

DATA_FOLDER = 'active1000'
files = os.listdir(DATA_FOLDER)
print(files)



ARBITRARY_INDEX = 0
filepath = os.path.join(DATA_FOLDER, files[ARBITRARY_INDEX])

# one way to load all events into memory
events = []
for line in open(filepath):
    events.append(json.loads(line.strip()))

print(json.dumps(events[ARBITRARY_INDEX], indent=4))

"""
{
    "eventId": 145694348,
    "city": "trondheim",
    "activeTime": 26,
    "url": "http://adressa.no",
    "referrerHostClass": "internal",
    "region": "sor-trondelag",
    "sessionStop": false,
    "userId": "cx:68n3jepy27wwrsstf4mvv33g:wt6yo8gwjk2l",
    "sessionStart": false,
    "deviceType": "Mobile",
    "time": 1483743602,
    "referrerUrl": "http://adressa.no",
    "country": "no",
    "os": "Android"
}
"""

try:
    df=pe.load_data("active1000")
    print("\nBasic statistics of the dataset...")
    pe.statistics(df)
except:
    logging.exception("Failed")

"""
Basic statistics of the dataset...
Total number of events (front page incl.): 2207608
Total number of events (without front page): 788931
Total number of documents: 20344
Sparsity: 3.878%
Total number of events (drop duplicates): 679355
Sparsity (drop duplicates): 3.339%

Describe by user:
            counts
count  1000.000000
mean    679.355000
std     333.619737
min      59.000000
25%     506.750000
50%     639.500000
75%     797.500000
max    7958.000000
"""