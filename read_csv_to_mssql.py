import pandas as pd
import pyodbc
import os
import numpy as np
import time

"""### Display settings (optional)"""

# Pandas display options
pd.options.display.max_colwidth = 255
pd.options.display.max_rows = 500
pd.options.display.width = 1000
pd.options.display.max_columns = 500


file_name = 'csv_data/aact_trial_info_us_4.7.csv'
# file_name = 'csv_data/key_criteria_4_6.csv'
file_name_output = os.path.basename(file_name.replace('.', '_')).replace('_csv','')
print(file_name_output)

# Import CSV
data = pd.read_csv(file_name, encoding='utf8')
print(data.columns)
columns = [str(column) for column in data.columns]
columns_str = ",".join(columns)
print(columns_str)

df = pd.DataFrame(data)

df2 = df.astype(object).where(pd.notnull(df), None)

print(df2.head(20))

# Connect to SQL Server
# conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
#                       'Server=192.168.1.35\UbuntuServerHaoUS;'
#                       'Database=trial_knowledge_base;'
#                       'Trusted_Connection=yes;')

driver='{ODBC Driver 17 for SQL Server}'
server='192.168.1.35'
db='trial_knowledge_base'
user='SA'
password='Dquest123$'

conn = pyodbc.connect('driver=%s;server=%s;database=%s;uid=%s;pwd=%s' % (driver, server, db, user, password))
cursor = conn.cursor()
#
# df.to_sql('aact_trial_info_us_4.7', conn, if_exists='replace', index = False)
# print('done')

sql ='''
CREATE TABLE [dbo].[%s] (
[nct_id] varchar(11),
[start_date] datetime,
[status] varchar(23),
[phase] varchar(15),
[study_type] varchar(32),
[brief_title] varchar(209),
[condition_names] varchar(153),
[intervention_names] varchar(221),
[minimum_age] varchar(4),
[maximum_age] varchar(4),
[gender] varchar(6),
[healthy_volunteers] varchar(26),
[facilities] varchar(8000)
)
'''% (file_name_output)

# sql = '''
# CREATE TABLE [dbo].%s (
# [nct_id] varchar(11),
# [disease_status] varchar(7),
# [exposure_status] varchar(4),
# [days_to_disease] varchar(4),
# [is_hospitalized] bit,
# [preg_status] bit,
# [oral_meds] bit
# )
# '''% (file_name_output)

# Create Table
cursor.execute(sql)

placeholders1 = ",".join("?" * len(columns))

sql2 = '''
INSERT INTO dbo.%s(%s)
                VALUES (%s)
''' % (file_name_output, columns_str, placeholders1)



# Insert DataFrame to Table
for index, row in df2.iterrows():
    # print(row)
    print(index)
    if 'facilities' in row and len(row['facilities'])>8000:
        row['facilities'] = row['facilities'][:8000]
    params = [row[column] for column in columns]
    cursor.execute(sql2, params)

conn.commit()
cursor.close()
conn.close()