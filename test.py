import pandas as pd

df = pd.read_pickle('tmp_response_date.pickle')

durations_df = df['durations']
durations_df = durations_df.apply(lambda a: pd.Series(a))
durations_df = durations_df.unstack().reset_index().rename(columns={0: 'durations', 'level_0': 'start','level_1': 'dest'})

distances_df = df['distances']
distances_df = distances_df.apply(lambda a: pd.Series(a))
distances_df = distances_df.unstack().reset_index().rename(columns={0: 'distance', 'level_0': 'start','level_1': 'dest'})

sources_df = df['sources']
sources_df = sources_df.apply(lambda a: pd.Series(a))


complete = durations_df.merge(
    distances_df,
    how='inner', on=['start', 'dest'])

complete = complete.loc[complete['start'] != complete['dest']]

complete = complete[['start', 'dest']].replace(
    sources_df['location'].map(lambda a: ','.join([str(k) for k in a])).to_dict()
)

# complete.loc[:,['Lon','Lat']] = (sources_df['location'].apply(lambda a: pd.Series((a[0],a[1]))).rename(columns={0: 'Lon', 1: 'Lat'}))
