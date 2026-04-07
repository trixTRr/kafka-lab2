from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when
import time
import json
import sys
import os

def get_memory_usage():
    # Получить использование памяти
    try:
        with open('/proc/self/status', 'r') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    # VmRSS показывает физическую память в kB
                    kb = int(line.split()[1])
                    return round(kb / 1024, 2)  # Конвертация в MB
    except:
        pass
    return 0

def get_system_memory():
    # Получить системную память
    try:
        with open('/proc/meminfo', 'r') as f:
            meminfo = {}
            for line in f:
                parts = line.split()
                if parts[0] in ['MemTotal:', 'MemAvailable:', 'MemFree:']:
                    meminfo[parts[0].rstrip(':')] = int(parts[1]) / 1024  # Конвертация в MB
            if meminfo:
                total = meminfo.get('MemTotal', 0)
                available = meminfo.get('MemAvailable', meminfo.get('MemFree', 0))
                used = total - available
                percent = (used / total * 100) if total > 0 else 0
                return {
                    'total_mb': round(total, 2),
                    'available_mb': round(available, 2),
                    'used_mb': round(used, 2),
                    'percent': round(percent, 1)
                }
    except:
        pass
    return {'total_mb': 0, 'available_mb': 0, 'used_mb': 0, 'percent': 0}

def main():
    experiment_name = sys.argv[1] if len(sys.argv) > 1 else "Unknown"
    optimization = sys.argv[2] if len(sys.argv) > 2 else "base"
    use_optimization = (optimization == "opt")
    
    print(f"\n{'='*60}")
    print(f"Starting {experiment_name}")
    print(f"Optimization: {'ON' if use_optimization else 'OFF'}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    # Замер памяти
    start_memory = get_memory_usage()
    start_system_memory = get_system_memory()
    
    print(f"\n INITIAL MEMORY STATE:")
    print(f"   Process memory: {start_memory} MB")
    if start_system_memory['total_mb'] > 0:
        print(f"System memory: {start_system_memory['used_mb']} MB / {start_system_memory['total_mb']} MB ({start_system_memory['percent']}%)")
    
    # Настройка Spark
    spark_builder = SparkSession.builder \
        .appName(experiment_name) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g")
    
    if use_optimization:
        spark_builder = spark_builder \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
    else:
        spark_builder = spark_builder \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "4")
    
    spark = spark_builder.getOrCreate()
    
    spark_start_memory = get_memory_usage()
    print(f"\nAFTER SPARK SESSION CREATION:")
    print(f"Process memory: {spark_start_memory} MB (+{spark_start_memory - start_memory} MB)")
    
    if use_optimization:
        print("Optimization: 8 shuffle partitions")
    else:
        print("Base config: 4 shuffle partitions")
    
    try:
        print("\nLoading data from HDFS...")
        load_start = time.time()
        df = spark.read.csv("hdfs://namenode:9000/user/data/dataset.csv", 
                            header=True, inferSchema=True)
        
        row_count = df.count()
        load_time = time.time() - load_start
        
        after_load_memory = get_memory_usage()
        print(f"Data loaded: {row_count:,} rows, {len(df.columns)} columns")
        print(f"Load time: {load_time:.2f}s")
        print(f"After data load: {after_load_memory} MB (+{after_load_memory - spark_start_memory} MB)")
        
        cache_time = 0
        if use_optimization and row_count < 500000:
            print("Caching data...")
            cache_start = time.time()
            from pyspark.storagelevel import StorageLevel
            
            before_cache_memory = get_memory_usage()
            df = df.persist(StorageLevel.MEMORY_AND_DISK)
            df.count()
            cache_time = time.time() - cache_start
            after_cache_memory = get_memory_usage()
            print(f"Cache time: {cache_time:.2f}s")
            print(f"After caching: {after_cache_memory} MB (+{after_cache_memory - before_cache_memory} MB)")
        
        # Выполнение запросов
        print("\n--- Executing analytics ---")
        query_start = time.time()
        
        before_query_memory = get_memory_usage()
        
        city_stats = df.groupBy("city").agg(
            count("*").alias("users"),
            avg("salary").alias("avg_salary"),
            avg("rating").alias("avg_rating")
        ).orderBy(col("users").desc())
        
        active = df.filter(col("is_active") == 1)
        active_cnt = active.count()
        
        df_with_cats = df.withColumn("category",
            when(col("purchases") < 5, "Low")
            .when(col("purchases") < 15, "Medium")
            .otherwise("High")
        )
        category_stats = df_with_cats.groupBy("category").agg(
            count("*").alias("count"),
            avg("salary").alias("avg_salary")
        )
        
        city_stats.count()
        category_stats.count()
        
        query_time = time.time() - query_start
        after_query_memory = get_memory_usage()
        
        duration = time.time() - start_time
        
        peak_memory = max(start_memory, spark_start_memory, after_load_memory, after_query_memory)
        if use_optimization and 'after_cache_memory' in locals():
            peak_memory = max(peak_memory, after_cache_memory)
        
        # Сохранение метрик
        metrics = {
            'experiment': experiment_name,
            'optimization': use_optimization,
            'duration_seconds': round(duration, 2),
            'load_time_seconds': round(load_time, 2),
            'query_time_seconds': round(query_time, 2),
            'cache_time_seconds': round(cache_time, 2),
            'partitions': 8 if use_optimization else 4,
            'rows_processed': row_count,
            'memory': {
                'start_mb': start_memory,
                'spark_start_mb': spark_start_memory,
                'after_load_mb': after_load_memory,
                'after_query_mb': after_query_memory,
                'peak_mb': peak_memory,
                'memory_increase_mb': round(after_query_memory - start_memory, 2)
            },
            'analysis': {
                'active_users_percent': round(active_cnt / row_count * 100, 1),
                'cities_count': city_stats.count()
            }
        }
        
        # 1. Сохраняем в текущую директорию
        with open('metrics.json', 'a') as f:
            f.write(json.dumps(metrics) + '\n')
        print(f"Metrics saved to ./metrics.json")
        
        # 2. Сохраняем в /results (если директория смонтирована)
        try:
            os.makedirs('/results', exist_ok=True)
            with open('/results/metrics.json', 'a') as f:
                f.write(json.dumps(metrics) + '\n')
            print(f"Metrics saved to /results/metrics.json")
        except:
            pass
        
        # Вывод RAM статистики
        print(f"\n{'='*60}")
        print(f"MEMORY USAGE SUMMARY")
        print(f"{'='*60}")
        print(f"Initial memory:        {start_memory} MB")
        print(f"After Spark init:      {spark_start_memory} MB (+{spark_start_memory - start_memory} MB)")
        print(f"After data load:       {after_load_memory} MB (+{after_load_memory - spark_start_memory} MB)")
        if use_optimization and 'after_cache_memory' in locals():
            print(f"After caching:         {after_cache_memory} MB")
        print(f"After queries:         {after_query_memory} MB")
        print(f"{'─'*60}")
        print(f"PEAK MEMORY:           {peak_memory} MB")
        print(f"TOTAL INCREASE:        {after_query_memory - start_memory} MB")
        print(f"{'='*60}")
        
        # Вывод результатов
        print("\n--- City Statistics (top 5) ---")
        city_stats.show(5)
        print(f"\n--- Active Users: {active_cnt:,} ({active_cnt/row_count*100:.1f}%) ---")
        print("\n--- Category Statistics ---")
        category_stats.show()
        
        print(f"\n{'='*60}")
        print(f"COMPLETED in {duration:.2f} seconds")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"\n ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if use_optimization:
            spark.catalog.clearCache()
        spark.stop()

if __name__ == "__main__":
    main()