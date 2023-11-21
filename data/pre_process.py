# %%
import pandas as pd
import os

data_path = './data'
output_path = './concated_data'


# %%
is_start = True
cnt = 0
for filename in sorted(os.listdir(data_path)):

    plat = filename.split('_')[0]
    # print(filename, plat)
    if cnt == 0:
        res = pd.read_csv(os.path.join(data_path, filename))
    else:
        res = pd.concat(
            [res, pd.read_csv(os.path.join(data_path, filename))], ignore_index=True)
    cnt += 1
    if cnt == 4:
        cnt = 0
        res.to_csv(os.path.join(output_path, plat + '.csv'))
# %%
cnt = 0
csv_lst = []
topics = ['Economy.csv', 'Microsoft.csv', 'Obama.csv', 'Palestine.csv']
for i, filename in enumerate(sorted(os.listdir(data_path))):
    cnt = i % 4
    if i < 4:
        csv_lst.append(pd.read_csv(os.path.join(data_path, filename)))
    else:
        csv_lst[cnt] = pd.concat([csv_lst[cnt], pd.read_csv(
            os.path.join(data_path, filename))], ignore_index=True)

for i, csv in enumerate(csv_lst):
    csv.to_csv(os.path.join(output_path, topics[i] + '.csv'))
# %%
csv = pd.read_csv('./data/News_Final.csv')

# print(csv[csv['Headline'].str.contains('""""""')].head())
# csv['Headline'] = csv['Headline'].str.replace('"""""', '')

csv.to_csv('./data/News_Final_clean.csv')

# %%
with open('./data/News_Final.csv', 'r') as ori:
    with open('./data/News_Final_n.csv', '+a') as tar:
        for line in ori:
            tar.write(line
                      .replace('""""""', '')
                      .replace('"""""', '')
                      .replace('""""', ''))

# %%
for i in range(0, 10, 3):

    print(i)

# %%

for i in range(0, 10, 3):
    for j in range(i, i + 3):

        print(i, j)
# %%
