import os
import pandas as pd
import re
import time
import numpy as np
import glob


def remove_non_numbers(cnpj):
    pattern = "\D"
    return re.sub(pattern, '', cnpj)


t0 = time.time()

years = sorted(os.listdir('data'))
for folder in years:
    path = 'data/{}'.format(folder)
    print("Formatting year {}".format(folder))
    for i, file in enumerate(sorted(os.listdir(path))):
        print("Formatting month {}".format(i + 1))
        file_path = '{}/{}'.format(path, file)
        df = pd.read_csv(file_path, sep=';')
        df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].apply(remove_non_numbers)
        df = df.replace(r'^\s*$', np.nan, regex=True)
        df = df.fillna('NULL')
        df = df.drop_duplicates()
        cnpjs = df.CNPJ_FUNDO.unique()
        for i, cnpj in enumerate(cnpjs):
            if i % 1000 == 0:
                print("{}th of {}".format(i + 1, len(cnpjs)))
            file_exists = glob.glob('funds/{}.csv'.format(cnpj))
            if file_exists:
                existing_file = file_exists[0]
                combined_csv = pd.concat([pd.read_csv(existing_file), df[df.CNPJ_FUNDO == cnpj]])
                combined_csv.to_csv('funds/{}.csv'.format(cnpj), index=False)
            else:
                df[df.CNPJ_FUNDO == cnpj].to_csv('funds/{}.csv'.format(cnpj), index=False)
    print("###############################")
        
t1 = time.time()
minutes = (t1 - t0)/60

print("Finished! It took {} minutes".format(minutes))
