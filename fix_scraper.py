import pandas as pd
import re

df = pd.read_csv('ai_suggested_overrides.csv')
for index, row in df.iterrows():
    name = str(row['name'])
    m_clean = re.sub(r'[^a-zA-Z\s]', '', name).strip().lower()
    if not m_clean or len(m_clean) < 3:
        df = df.drop(index)

df.to_csv('ai_suggested_overrides.csv', index=False)
