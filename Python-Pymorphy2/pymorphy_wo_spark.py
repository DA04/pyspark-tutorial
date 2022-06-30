# Testing without pyspark
import re
import pandas as pd
import pymorphy2
morph = pymorphy2.MorphAnalyzer()
import numpy as np

with open('./books/beg.txt', 'r', encoding='utf-8') as f:
    content = f.read()
    lst = content.split(' ')
    t_list = []
    for a in lst:
        if re.match("[А-Я][а-я]*", a) and len(a)>3:
            t_list.append(a)
    df = pd.DataFrame({'word':t_list})
df['count'] = df.groupby('word')['word'].transform('count')
df['normal'] = df['word'].apply(lambda x: morph.parse(x)[0].normal_form)
df['normal'] = df['normal'].str.title()
df = pd.pivot_table(df, index=['normal'], values=['count','word'], aggfunc={'count':np.sum, 'word':lambda x: list(x)})
df = df.reset_index()
df['tag'] = df['normal'].apply(lambda x: morph.parse(x)[0].tag)
df['tag'] = df['tag'].astype(str)
df = df[df['tag'].str.contains('Geo')]
df = df.sort_values(by='count', ascending=False)
df