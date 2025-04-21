# spark_filter_example.py

from pyspark import SparkConf, SparkContext

def main():
    # Configure your app name
    conf = SparkConf().setAppName("SparkFilterExample")
    sc = SparkContext(conf=conf)
    
    # Replace this with your actual input path (local file or HDFS/S3 URI)
    input_path = "data/input.txt"
    
    # Load the text data
    lines = sc.textFile(input_path)
    
    # Define a keyword to filter by
    keyword = "error"
    
    # Filter lines containing the keyword (case‑insensitive)
    filtered = lines.filter(lambda line: keyword.lower() in line.lower())
    
    # Collect and print results
    results = filtered.collect()
    print(f"Lines containing '{keyword}':")
    for line in results:
        print(line)
    
    # Clean up
    sc.stop()

if __name__ == "__main__":
    main()
