from datetime import datetime
def handler(input:dict,context:object):
    if(not context.env):
        context.env = {
        "avg-60sec":[[input["cpu_percent-"+str(i)]] for i in range(1,15)],
        "avg-60min":[[input["cpu_percent-"+str(i)]] for i in range(1,15)],
        "vm-memory":[input["virtual_memory-percent"]],
        "timestamp-60sec":[input["timestamp"]],
        "timestamp-60min":[input["timestamp"]],
        "real-timestamp":[input["timestamp"]]
          }
    else:
      now = datetime.strptime(input["timestamp"],"%Y-%m-%d %H:%M:%S.%f")
      dsec = datetime.strptime(context.env["timestamp-60sec"][-1],"%Y-%m-%d %H:%M:%S.%f")
      dmin = datetime.strptime(context.env["timestamp-60min"][-1],"%Y-%m-%d %H:%M:%S.%f")
      d3 = now-dsec
      d4 = now-dmin
      context.env["vm-memory"].append(input["virtual_memory-percent"])
      context.env["real-timestamp"].append(input["timestamp"])
      if(d3.seconds>=60):
          for i in range(14):
              average = (sum(context.env["avg-60sec"][i])+input["cpu_percent-"+str(i+1)])/(len(context.env["timestamp-60sec"])+1)
              context.env["avg-60sec"][i].append(average)
          context.env["timestamp-60sec"].append(input["timestamp"])
      if(d4.seconds>=3600):
          for i in range(14):
              average = (sum(context.env["avg-60min"][i])+input["cpu_percent-"+str(i+1)])/(len(context.env["timestamp-60min"])+1)
              context.env["avg-60min"][i].append(average)
          context.env["timestamp-60min"].append(input["timestamp"])

    mymetrics = {
      "avg-usage-cpu-60sec":context.env["avg-60sec"],
      "timestamp-60sec":context.env["timestamp-60sec"],
      "avg-usage-cpu-60min":context.env["avg-60min"],
      "virtual-memory-percent-rt":context.env["vm-memory"],
      "timestamp-60min":context.env["timestamp-60min"],
      "timestamp-rt":context.env["real-timestamp"]
    }
    
    return mymetrics