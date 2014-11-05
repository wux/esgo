
import numpy
import math
# grep 'Query took' log50 | grep -v ms | sed 's/s//g' | awk ' { total += $3 } END { print total; print NR }'

logs = [ "log%d" % (i + 1) for i in range(40)]
filter = "Query took"

#logs=["log1"]

#print(logs)
for log in logs:
    latency = []
    for line in open(log, 'r'):
        if line.find(filter) == -1:
            continue
        info = line.split()
        time = info[2][:-1]
        scale = 1000.0
        if time.endswith('m'):
            scale = 1.0
            time = time[:-1]
        latency.append(float(time) * scale)
    # The first request is for fetching all the names.
    latency = latency[1:]
    #print(log, len(latency), math.fsum(latency) / len(latency), max(latency), min(latency))
    print(latency, log)
    print("%f %f %f" % (math.fsum(latency) / len(latency), max(latency), min(latency)))
a="""
    for i in range(1, len(data)):
        delta = data[i] - data[i - 1]
        s = delta * 1e9 / (time[i] - time[i-1])
        col.append(s)
    form.append(col)

num_c = len(form)
num_r = max(len(col) for col in form)
f = open("log", "w+")
for r in range(num_r):
    for c in range(num_c):
        if r < len(form[c]):
            f.write("%-6.2f " % form[c][r])
        else:
            f.write("%-6.2f " % 0.0)
    f.write("\n")
"""
