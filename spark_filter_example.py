# spark_filter_example.py

import sys
from pyspark import SparkConf, SparkContext

def main(input_path):
    conf = SparkConf().setAppName("SparkFilterExample")
    sc   = SparkContext(conf=conf)

    lines   = sc.textFile(input_path)
    keyword = "error"
    filtered = lines.filter(lambda line: keyword.lower() in line.lower())
    results  = filtered.collect()

    print(f"Lines containing '{keyword}':")
    for line in results:
        print(line)

    sc.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark_filter_example.py <input_path>")
        sys.exit(1)

    main(sys.argv[1])
