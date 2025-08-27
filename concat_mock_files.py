import pandas as pd
import glob


csv_files = glob.glob('/home/PycharmProjects/de_zoomcamp/dbdata/mock_data/*.csv')

df_list = [pd.read_csv(file) for file in csv_files]
concatenated_df = pd.concat(df_list, ignore_index=True)

concatenated_df.to_csv('MOCK_DATA_USERS.csv', index=False)

# awk '(NR==1) || (FNR!=1)' *.csv > concatenated_output.csv
# It skips the header for all files except the first one