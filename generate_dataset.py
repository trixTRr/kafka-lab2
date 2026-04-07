import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Настройки
np.random.seed(42)
n_rows = 150000

# Генерация данных
data = {
    'user_id': np.arange(1, n_rows + 1),
    'age': np.random.randint(18, 80, n_rows),
    'salary': np.random.normal(50000, 20000, n_rows).astype(int),
    'city': np.random.choice(['Moscow', 'SPB', 'Novosibirsk', 'Ekaterinburg', 'Kazan', 'Nizhny'], n_rows),
    'purchases': np.random.poisson(10, n_rows),
    'rating': np.random.uniform(1, 5, n_rows).round(2),
    'is_active': np.random.choice([0, 1], n_rows, p=[0.3, 0.7]),
    'registration_date': [datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1095)) for _ in range(n_rows)]
}

df = pd.DataFrame(data)
df['salary'] = np.abs(df['salary'])
df.to_csv('C:/L_a_b/dataset.csv', index=False)
print(f"Dataset created: {len(df)} rows, {len(df.columns)} columns")
print(df.dtypes)