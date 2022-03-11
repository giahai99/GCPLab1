import json
import random
import string
import time
import sys

if sys.argv[1] == '':
    input_topic = "projects/my-project-demo-338104/topics/uc1-input-topic-40"
else:
    input_topic = sys.argv[1]

print(input_topic)