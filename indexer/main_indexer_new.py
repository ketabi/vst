from Loader.GeneralSheetLoader import GeneralSheetLoader
from elasticsearch import Elasticsearch
import urllib3
from base_logger import logger


logger.warning('This is a Warning')

files = [
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
    (10145, 'LifeStyle', 1),
    # (10145, 'MobileUse', 1),
    # (10145, 'OralHealth', 1),
    # (10145, 'Pesticide', 1),
    # (10145, 'PhysicalActivity', 1),
    # (10145, 'PhysicalExam', 1),
    # (10145, 'SleepAssessment', 1),
    # (10145, 'Socioeconomic', 1),
    # (10145, 'WaterUsed', 1),
    #
    # (10160, 'GFR_Score', 0),
    # (16612, 'UsedDrug', 0),
    # (10884, 'EmploymentHistory', 0),
    # (40773, 'HFHistory', 0),
    # (265656, 'FamilyHistory', 0),
    # (140185, 'FoodHabit', 0),
    # (110935, 'SupplementUsed', 0),
]

sheets = {}


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

es = Elasticsearch(["https://elastic:elastic-password@localhost:9200/"], verify_certs=False)


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

DATA_FOLDER_PREFIX = "./data/summer-1401/"
# DATA_FOLDER_PREFIX = "./data/csv/"


def sheet_loader(f):
    rows_count, sheet_name, skipped_rows = f
    df_loader = None
    if rows_count <= 10145:
        df_loader = GeneralSheetLoader(f"{DATA_FOLDER_PREFIX}{sheet_name}.xlsx", sheet_name, skiprows=skipped_rows)
    return sheet_name, df_loader


for idx, f in enumerate(files):
    sheet_name, df_loader = sheet_loader(f)
    if df_loader is not None:
        # index_in_elastic(df_loader.df, f'new_cohort_{sheet_name.lower()}')
        sheets[sheet_name] = df_loader
        #if idx > 0:
        #    df.merge

# index_in_elastic(df_loader.df, f'new_cohort_{sheet_name.lower()}')
