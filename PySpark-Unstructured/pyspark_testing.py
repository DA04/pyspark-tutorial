# Testing with pyspark

from pyspark.sql.functions import col, split, explode, regexp_extract, lower, length
import pandas as pd
import numpy as np
import pymorphy2
morph = pymorphy2.MorphAnalyzer()
import os
from os import listdir

def words_morph(df):
    df['normal'] = df['word'].apply(lambda x: morph.parse(x)[0].normal_form)
    df['normal'] = df['normal'].str.title()
    df = pd.pivot_table(df, index=['normal'], values=['count','word'], aggfunc={'count':np.sum, 'word':lambda x: list(x)})
    df = df.reset_index()
    df['tag'] = df['normal'].apply(lambda x: morph.parse(x)[0].tag)
    df['tag'] = df['tag'].astype(str)
    df = df[df['tag'].str.contains('Geo')]
    df = df.sort_values(by='count', ascending=False)
    return df

def extract_geography(filename):
    df = spark.read.text(filename)
    lines = df.select(split(col("value"), " ").alias("lines"))
    words = lines.select(explode(col("lines")).alias("word"))
    words = words.select(regexp_extract(col("word"), "[А-Я][а-я]*", 0).alias("word"))
    words = words.where(col("word")!="")
    words = words.where(length(col("word"))>3)
    words = words.groupby("word").count()
    words_df = words.toPandas()
    return words_morph(words_df)

def main():
    path = './books_encoded/'
    df_main = pd.DataFrame()
    n_count = 1
    for book in listdir(path):
        print(book, n_count)
        filename = path+'/'+book
        df = extract_geography(filename)
        df['source'] = os.path.basename(filename)
        df_main = df_main.append(df, ignore_index=True)
        n_count+=1
    df_main.to_csv('results.csv')
    
if __name__=='__main__':
    import cProfile
    cProfile.run('main()', 'output_spark.dat')

    import pstats
    from pstats import SortKey

    with open('output_time_spark.txt', 'w') as f:
        p = pstats.Stats('output_spark.dat', stream=f)
        p.sort_stats('time').print_stats()
    
    with open('output_calls_spark.txt', 'w') as f:
        p = pstats.Stats('output_spark.dat', stream=f)
        p.sort_stats('calls').print_stats()