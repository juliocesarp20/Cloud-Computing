import os
from threading import Timer
import redis
import usermodule
import json
from datetime import datetime
def change_status():
    input = json.loads(r.get(new_context.input_key))
    if(new_context.last_execution!=""):
        last_context = datetime.strptime(new_context.last_execution,"%Y-%m-%d %H:%M:%S.%f")
        last_redis = datetime.strptime(input["timestamp"],"%Y-%m-%d %H:%M:%S.%f")
    if(new_context.last_execution=="" or (last_context-last_redis).seconds != 0):
        output = usermodule.handler(input,new_context)
        r.set(new_context.output_key,json.dumps(output))
        new_context.last_execution=input["timestamp"]
        new_context.function_getmtime = input["timestamp"]
    Timer(0.2, change_status).start()

class context:
    def __init__(self,host,port,input_key,output_key):
        self.env = {}
        self.host = host
        self.port = port
        self.input_key = input_key
        self.output_key = output_key
        self.function_getmtime = ""
        self.last_execution = ""

if __name__ == '__main__':
    host = os.environ["REDIS_HOST"]
    port = os.environ["REDIS_PORT"]
    input_key = os.environ["REDIS_INPUT_KEY"]
    output_key = os.environ["REDIS_OUTPUT_KEY"]
    new_context = context(host,port,input_key,output_key)
    r = redis.Redis(host=host,port=port) 
    Timer(0.2, change_status).start()
