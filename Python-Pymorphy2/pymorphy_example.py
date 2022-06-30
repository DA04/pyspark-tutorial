# Pymorphy https://pypi.org/project/pymorphy2/
import pandas as pd
import numpy as np
import pymorphy2
morph = pymorphy2.MorphAnalyzer()

df = pd.read_csv('./csv/results.csv', names=['word', 'count'])
df['normal'] = df['word'].apply(lambda x: morph.parse(x)[0].normal_form)
df['normal'] = df['normal'].str.title()
df = pd.pivot_table(df, index=['normal'], values=['count','word'], aggfunc={'count':np.sum, 'word':lambda x: list(x)})
df = df.reset_index()
df['tag'] = df['normal'].apply(lambda x: morph.parse(x)[0].tag)
df['tag'] = df['tag'].astype(str)
df = df[df['tag'].str.contains('Geo')]
df = df.sort_values(by='count', ascending=False)
df
# # df.to_csv('test_count.csv')
