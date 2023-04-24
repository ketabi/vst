from Loader.GeneralSheetLoader import GeneralSheetLoader
from elasticsearch import Elasticsearch
import urllib3
from base_logger import logger
from functools import reduce # only in Python 3
import os
import pandas as pd

es_user = os.environ.get('VST_ELASTIC_USER', 'elastic')
es_pass = os.environ.get('VST_ELASTIC_PASS', 'elastic_pass')
data_path = os.environ.get('VST_DATA_PATH', '../../data-fasa/summer-1401/')


logger.warning('This is a Warning')


files = [
        ("AmputationHistory.xlsx", 1),
        ("AnthropometericExam.xlsx", 1),
        ("BloodPressure.xlsx", 1),
("ChronicDisease1.xlsx", 1),
("ChronicDisease2.xlsx", 1),
("EmploymentHistory.xlsx", 1),
("EmploymentStatus.xlsx", 1),
("FamilyHistory.xlsx", 1),
("Final_MET_Score.xlsx", 1),
("GeneralData.xlsx", 1),
("GFR_Score.xlsx", 1),
# ("GPD.xlsx", 1),
("Gynecologist.xlsx", 1),
("Habit.xlsx", 1),
("HFHistory.xlsx", 1),
("ImportantMedicationAndDiseases.xlsx", 1),
("LifeStyle.xlsx", 1),
("NBS.xlsx", 1),
("OralHealth.xlsx", 1),
("PhysicalActivity.xlsx", 1),
("PhysicalExam.xlsx", 1),
("SleepAssessment.xlsx", 1),
("Socioeconomic.xlsx", 1),
("UsedDrug.xlsx", 1),
("Wealth_Score.xlsx", 1)
    ]


    # (9536,  'labTestresult', 1),
    # (10145, 'AnthropometericExam', 1),
    # (10145, 'BioBank', 1),
    # (10145, 'BloodPressure', 1),
    # (10145, 'ChronicDisease1', 1),
    # (10145, 'ChronicDisease2', 1),
    # (10145, 'EmploymentStatus', 1),
    # (10145, 'Final_MET_Score', 1),
    # (10145, 'Gynecologist', 1),
    # (10145, 'Habit', 1),
    # (10145, 'LifeStyle', 1),
    #(10145, 'MobileUse', 1),
    #(10145, 'OralHealth', 1),
    #(10145, 'Pesticide', 1),
    #(10145, 'PhysicalActivity', 1),
    #(10145, 'PhysicalExam', 1),
    #(10145, 'SleepAssessment', 1),
    #(10145, 'Socioeconomic', 1),
    #(10145, 'WaterUsed', 1),
    #
    # (10160, 'GFR_Score', 0),
    # (16612, 'UsedDrug', 0),
    # (10884, 'EmploymentHistory', 0),
    # (40773, 'HFHistory', 0),
    # (265656, 'FamilyHistory', 0),
    # (140185, 'FoodHabit', 0),
    # (110935, 'SupplementUsed', 0),
#]

sheets = {}


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

elastic_url = f'https://{es_user}:{es_pass}@localhost:9200/'
print(f'elastic_url: {elastic_url}')
es = Elasticsearch([elastic_url], verify_certs=False, request_timeout=30)


def index_in_elastic(df, index_name):
    print(f'{df.shape} [index_name:{index_name}] will be indexed in elastic .............')
    ignored = 0
    for idx, row in df.iterrows():
        d = row.to_json()
        try:
            es.index(index=index_name, id=idx, document=d)
        except Exception as e:
            ignored += 1
            print(e, idx)
    print(f'{ignored} rows failed to be indexed')

# DATA_FOLDER_PREFIX = "./data/csv/"


def sheet_loader(f):
    sheet_name, skipped_rows = f
    print(f'{"-"*10}{sheet_name}')
    df_loader = GeneralSheetLoader(f'{data_path}{sheet_name}', sheet_name, skiprows=skipped_rows)
    return sheet_name.split('.')[0], df_loader
    

data_frames = []
dfs_unique = []
unique_dfs_name = []
cols = []

for idx, f in enumerate(files):
    sheet_name, loader = sheet_loader(f)
    if loader.duplicated_count == 0:
        unique_dfs_name.append(sheet_name)
        dfs_unique.append(loader.df)

    cols.extend(loader.df.columns)
    print(f'{sheet_name}, {loader.df.shape}')
    if loader is not None:
        index_in_elastic(loader.df, f'new_cohort_2_{sheet_name.lower()}')
        # sheets[sheet_name] = df_loader
        data_frames.append(loader.df)
        #if idx > 0:
        #    df.merge

print(f'{len(unique_dfs_name)} dfs has unique IRPCs')
print(f'{len(data_frames)} total dfs')
print(f'{len(cols)} cols, {len(set(cols))} uniq cols')
df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['IRPC'],
                                            how='outer'), dfs_unique)
index_in_elastic(df_merged, 'new_cohort_2_all')
logger.warning(df_merged.shape)
# print(data_frames)
