GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Создание директории для результатов
RESULTS_DIR="results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR/metrics"
mkdir -p "$RESULTS_DIR/logs"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   ЗАПУСК ЭКСПЕРИМЕНТОВ${NC}"
echo -e "${BLUE}   Результаты будут сохранены в: $RESULTS_DIR${NC}"
echo -e "${BLUE}========================================${NC}"

source venv/bin/activate

# Функция для запуска эксперимента
run_experiment() {
    local compose_file=$1
    local exp_name=$2
    local optimization=$3
    local run_id=$4
    
    echo -e "\n${GREEN}=== Starting $exp_name (Opt: $optimization) ===${NC}"
    echo -e "${YELLOW}Run ID: $run_id${NC}"
    
    # Очистка старых контейнеров
    docker compose -f $compose_file down 2>/dev/null
    
    # Запуск кластера
    docker compose -f $compose_file up -d
    
    echo -e "${YELLOW}Waiting for Hadoop (10 sec)...${NC}"
    sleep 10
    
    # Проверка готовности
    echo -e "${YELLOW}Checking Hadoop status...${NC}"
    docker exec namenode hdfs dfsadmin -report | head -5
    
    # Копирование данных в HDFS
    echo -e "${YELLOW}Copying data to HDFS...${NC}"
    docker cp dataset.csv namenode:/dataset.csv
    docker exec namenode hdfs dfs -mkdir -p /user/data 2>/dev/null
    docker exec namenode hdfs dfs -rm -f /user/data/dataset.csv 2>/dev/null
    docker exec namenode hdfs dfs -put -f /dataset.csv /user/data/
    
    # Запуск Spark и сохранение логов
    echo -e "${YELLOW}Running Spark...${NC}"
    
    docker run --rm \
        --network container:namenode \
        -v $(pwd):/app \
        -v $(pwd)/$RESULTS_DIR:/results \
        bitnamilegacy/spark:3.5.0 \
        spark-submit \
        --master local[*] \
        --driver-memory 1g \
        /app/spark_app_auto.py "$exp_name" "$optimization" \
        2>&1 | tee "$RESULTS_DIR/logs/${exp_name}_${run_id}.log"
    
    # Копирование метрик
    mv metrics_*.json "$RESULTS_DIR/metrics/" 2>/dev/null
    mv all_metrics.jsonl "$RESULTS_DIR/" 2>/dev/null
    
    # Остановка кластера
    echo -e "${YELLOW}Stopping cluster...${NC}"
    docker compose -f $compose_file down
    sleep 5
}

# Генерация ID запуска
RUN_ID=$(date +%Y%m%d_%H%M%S)

# Запуск 4 экспериментов
run_experiment "docker-compose-hadoop-1dn.yml" "1DN_Base" "base" "$RUN_ID"
run_experiment "docker-compose-hadoop-1dn.yml" "1DN_Optimized" "opt" "$RUN_ID"
run_experiment "docker-compose-hadoop-3dn.yml" "3DN_Base" "base" "$RUN_ID"
run_experiment "docker-compose-hadoop-3dn.yml" "3DN_Optimized" "opt" "$RUN_ID"

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}   ВСЕ ЭКСПЕРИМЕНТЫ ЗАВЕРШЕНЫ${NC}"
echo -e "${GREEN}   Результаты сохранены в: $RESULTS_DIR${NC}"
echo -e "${GREEN}========================================${NC}"

# Автоматическое построение графиков
echo -e "\n${BLUE}Построение графиков...${NC}"
python3 auto_plot_results.py "$RESULTS_DIR"