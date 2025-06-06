import sys
import random
import threading
import time
from collections import defaultdict, Counter
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.streaming import StreamingContext


# Hash function factory for Count-Min and Count Sketches
def generate_hash_function(a, b, p, C):
    return lambda x: ((a * x + b) % p) % C


# Sign hash for Count Sketch
def generate_sign_function(a, b, p):
    return lambda x: 1 if ((a * x + b) % p) % 2 == 0 else -1


class CountMinSketch:
    def __init__(self, D, W):
        self.D = D
        self.W = W
        self.p = 8191
        self.table = [[0] * W for _ in range(D)]
        self.hashes = [
            generate_hash_function(random.randint(1, self.p - 1), random.randint(0, self.p - 1), self.p, W)
            for _ in range(D)
        ]

    def add(self, x):
        for i in range(self.D):
            idx = self.hashes[i](x)
            self.table[i][idx] += 1

    def estimate(self, x):
        return min(self.table[i][self.hashes[i](x)] for i in range(self.D))


class CountSketch:
    def __init__(self, D, W):
        self.D = D
        self.W = W
        self.p = 8191
        self.table = [[0] * W for _ in range(D)]
        self.hashes = [
            generate_hash_function(random.randint(1, self.p - 1), random.randint(0, self.p - 1), self.p, W)
            for _ in range(D)
        ]
        self.signs = [
            generate_sign_function(random.randint(1, self.p - 1), random.randint(0, self.p - 1), self.p)
            for _ in range(D)
        ]

    def add(self, x):
        for i in range(self.D):
            idx = self.hashes[i](x)
            sign = self.signs[i](x)
            self.table[i][idx] += sign

    def estimate(self, x):
        estimates = [self.table[i][self.hashes[i](x)] * self.signs[i](x) for i in range(self.D)]
        estimates.sort()
        mid = len(estimates) // 2
        return estimates[mid] if len(estimates) % 2 == 1 else (estimates[mid - 1] + estimates[mid]) / 2


def process_batch(time, batch):
    batch_size = batch.count()
    # If we already have enough points (> THRESHOLD), skip this batch.
    if streamLength[0]>=THRESHOLD:
        return
    streamLength[0] += batch_size
    # Extract the distinct items from the batch
    batch_items = batch.map(lambda s: (int(s), 1)).reduceByKey(lambda i1, i2: 1).collectAsMap()

    for item in batch_items:
        true_counts[item] += 1
        cms.add(item)
        cs.add(item)
    streamLength[0] += len(batch_items)
           
    # If we wanted, here we could run some additional code on the global histogram
    # if batch_size > 0:
    #     print("Batch size at time [{0}] is: {1}".format(time, batch_size))

    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()


if __name__ == '__main__':
    assert len(sys.argv) == 6, "USAGE: port, threshold, D, W, K"
    portExp = int(sys.argv[1])
    THRESHOLD = int(sys.argv[2])
    D = int(sys.argv[3])
    W = int(sys.argv[4])
    K = int(sys.argv[5])

    conf = SparkConf().setMaster("local[*]").setAppName("G11HW3")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)
    ssc.sparkContext.setLogLevel("ERROR")

    stopping_condition = threading.Event()
    streamLength = [0]
    true_counts = defaultdict(int)
    cms = CountMinSketch(D, W)
    cs = CountSketch(D, W)

    print(f'Port = {portExp} T = {THRESHOLD} D = {D} W = {W} K = {K}')

    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(lambda time, rdd: process_batch(time, rdd))

    ssc.start()
    stopping_condition.wait()
    ssc.stop(False, False)

    print(f"Number of processed items = {streamLength[0]}")
    print(f"Number of distinct items  = {len(true_counts)}")

    # Identify top-K heavy hitters
    sorted_items = sorted(true_counts.items(), key=lambda x: -x[1])
    if len(sorted_items) == 0:
        sys.exit(0)

    phiK = sorted_items[K - 1][1] if len(sorted_items) >= K else sorted_items[-1][1]
    heavy_hitters = [item for item, count in sorted_items if count >= phiK]

    print(f'Number of Top-K Heavy Hitters = {len(heavy_hitters)}')

    cm_errors = []
    cs_errors = []

    for item in heavy_hitters:
        true_freq = true_counts[item]
        cm_est = cms.estimate(item)
        cs_est = cs.estimate(item)
        cm_errors.append(abs(cm_est - true_freq) / true_freq)
        cs_errors.append(abs(cs_est - true_freq) / true_freq)

    print(f'Avg Relative Error for Top-K Heavy Hitters with CM = {sum(cm_errors)/len(cm_errors)}')
    print(f'Avg Relative Error for Top-K Heavy Hitters with CS = {sum(cs_errors)/len(cs_errors)}')

    if K <= 10:
        print('Top-K Heavy Hitters')
        for item in heavy_hitters:
            print(f'Item {item} True Frequency = {true_counts[item]} Estimated Frequency with CM = {cms.estimate(item)}')
