# spark_filter_example.py

import sys
import re
from pyspark import SparkConf, SparkContext

def main(input_path):
    # Initialize Spark
    conf = SparkConf().setAppName("SparkFilterExample")
    sc   = SparkContext(conf=conf)

    # Read the file
    lines = sc.textFile(input_path)

    # Compile a regex that matches "error" or "warning", case-insensitive
    pattern = re.compile(r"(error|warning)", re.IGNORECASE)

    # Filter lines matching the pattern
    filtered = lines.filter(lambda line: bool(pattern.search(line)))
    results  = filtered.collect()

    # Print out
    print("Lines containing 'error' or 'warning':")
    for line in results:
        print(line)

    sc.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark_filter_example.py <input_path>")
        sys.exit(1)

    main(sys.argv[1])
