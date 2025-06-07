import argparse
import random as r
import threading
from collections import defaultdict

from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.streaming import StreamingContext

HOSTNAME = 'algo.dei.unipd.it'


def generate_hash_function(C: int, p: int = 8191):
    a = r.randint(1, p - 1)
    b = r.randint(0, p - 1)
    return lambda x: ((a * x + b) % p) % C


def generate_sign_function(p: int = 8191):
    a = r.randint(1, p - 1)
    b = r.randint(0, p - 1)
    return lambda x: 1 if ((a * x + b) % p) % 2 == 0 else -1


class CountMinSketch:
    def __init__(self, D, W):
        self.D = D
        self.W = W
        self.table = [[0] * W for _ in range(D)]
        self.hashes = [generate_hash_function(W) for _ in range(D)]

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
        self.table = [[0] * W for _ in range(D)]
        self.hashes = [generate_hash_function(W) for _ in range(D)]
        self.signs = [generate_sign_function() for _ in range(D)]

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


def process_batch(time, batch, T, streamLength, histogram, tc, cm, cs, stopping_condition):

    # Skip if we processed enough items from stream
    if streamLength[0] >= T:
        return
    streamLength[0] += batch.count()

    # Extract item counts from the batch
    batch_items = batch.map(lambda s: (int(s), 1)).reduceByKey(lambda x, y: x + y).collectAsMap()

    # Update the histogram, true counts, and sketches
    for key, count in batch_items.items():
        if key not in histogram:
            histogram.add(key)
        tc[key] += count
        for _ in range(count):
            cm.add(key)
            cs.add(key)

    # Set the stopping condition if we reached the target number of items
    if streamLength[0] >= T:
        stopping_condition.set()


def main(portExp: int, T: int, D: int, W: int, K: int):

    # Print command-line arguments
    print(f'Port = {portExp} T = {T} D = {D} W = {W} K = {K}')

    # Setup Spark
    conf = SparkConf().setMaster('local[*]').setAppName('G11HW3')
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)
    ssc.sparkContext.setLogLevel('ERROR')

    # Setup semaphore for clean shutdown
    stopping_condition = threading.Event()

    # Setup data structures
    streamLength = [0]
    histogram = set()
    tc = defaultdict(int)
    cm = CountMinSketch(D, W)
    cs = CountSketch(D, W)

    # Setup socket stream
    stream = ssc.socketTextStream(HOSTNAME, portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(
        lambda time, rdd: process_batch(time, rdd, T, streamLength, histogram, tc, cm, cs, stopping_condition)
    )

    # Read the socket stream until target number of items is reached
    ssc.start()
    stopping_condition.wait()
    ssc.stop(False, False)

    # Identify heavy hitters
    sorted_tc = sorted(tc.items(), key=lambda x: -x[1])
    phi_K = sorted_tc[K - 1][1] if len(sorted_tc) >= K else sorted_tc[-1][1]
    heavy_hitters = [item for item, count in tc.items() if count >= phi_K]

    # Compute relative errors
    cm_errors = []
    cs_errors = []
    for item in heavy_hitters:
        tc_item = tc[item]
        cm_est = cm.estimate(item)
        cs_est = cs.estimate(item)
        cm_errors += [abs(tc_item - cm_est) / tc_item]
        cs_errors += [abs(tc_item - cs_est) / tc_item]

    # Print results
    print(f"Number of processed items = {streamLength[0]}")
    print(f"Number of distinct items  = {len(histogram)}")
    print(f'Number of Top-K Heavy Hitters = {len(heavy_hitters)}')
    print(f'Avg Relative Error for Top-K Heavy Hitters with CM = {sum(cm_errors)/len(cm_errors)}')
    print(f'Avg Relative Error for Top-K Heavy Hitters with CS = {sum(cs_errors)/len(cs_errors)}')
    if K <= 10:
        print('Top-K Heavy Hitters:')
        for item in sorted(heavy_hitters):
            print(f'Item {item} True Frequency = {tc[item]} Estimated Frequency with CM = {cm.estimate(item)}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('portExp', type=int, help='Port number')
    parser.add_argument('T', type=int, help='Target number of items to process')
    parser.add_argument('D', type=int, help='Number of rows of each sketch')
    parser.add_argument('W', type=int, help='Number of columns of each sketch')
    parser.add_argument('K', type=int, help='Number of top frequent items of interest')

    args = parser.parse_args()
    main(args.portExp, args.T, args.D, args.W, args.K)
