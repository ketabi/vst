import pandas as pd
# from cohort.dataProcessing.sheets.columnsName import IRPC_COLUMN_NAME, GENDER_COLUMN_NAME
import numpy as np

from base_logger import logger

logger.info('HIIIIIIIIIIIIIIII')




MALE_GENDER = 1
FEMALE_GENDER = 2


class GeneralSheetLoader(object):

    def __init__(self, file_path, sheet_name, data_types=None, skiprows=0):
        logger.info(f'+======================== creating a loader: {sheet_name}')
        self.file_path = file_path
        self.sheet_name = sheet_name
        self.removed_cols = []
        self.csv_data = self._read_data(data_types, skiprows)
        self.df = self.csv_data
        self._set_unique_ids()
        self.df = self._process_irpc()
        self.df = self._remove_duplicate()
        self.df = self._process_date_fields()
        # self.df[IRPC_COLUMN_NAME] = self.df[IRPC_COLUMN_NAME].fillna(-1).astype(int)
        self.df = self._rename_cols()

    def _read_data(self, data_types, skip_rows):
        # logger.info('='*100)
        # logger.info()
        # logger.info("Reading this file  {} ".format(self.file_path))
        if self.file_path.endswith('.csv'):
            # parse csv with pandas
            csv_data = pd.read_csv(self.file_path,
                                   skiprows=skip_rows,  # number of lines to skip (int) at the start of the file.
                                   header=0,  # Row number(s) to use as the column names, and the start of the data.
                                   dtype=data_types
                                   )
        elif self.file_path.endswith('.xlsx'):
            csv_data = pd.read_excel(self.file_path,
                                     skiprows=skip_rows,  # number of lines to skip (int) at the start of the file.
                                     header=0,  # Row number(s) to use as the column names, and the start of the data.
                                     dtype=data_types
                                     )
        # iterator=True,
        # converters={
        #     'no': int,
        # }
        # # chunksize=chunksize
        # )

        logger.info('File {} has this shape:{}'.format(self.file_path, csv_data.shape))
        logger.info('columns: {}'.format(csv_data.columns))

        self.csv_data = csv_data

        return csv_data

    def _set_unique_ids(self):
        """
        List Unique Values In A pandas Column

        #List unique values in the df['name'] column
        df.name.unique()

        https://chrisalbon.com/python/data_wrangling/pandas_list_unique_values_in_column
        :return:
        """
        # self.ids_unique = self.csv_data[IRPC_COLUMN_NAME].unique()
        # logger.info('No. unique ids :{}'.format(len(self.ids_unique)))

    def describe(self):
        # -----------------------------------------------------------------------
        # DESCRIBE
        # -----------------------------------------------------------------------
        logger.info('\n' + '-' * 50)
        logger.info('Describe')
        logger.info('-' * 50)

        logger.info(self.csv_data.describe())

    def info(self):
        # -----------------------------------------------------------------------
        # INFO
        # -----------------------------------------------------------------------
        logger.info('\n' + '-' * 50)
        logger.info('info')
        logger.info('-' * 50)

        logger.info(self.csv_data.info())

    def value_count(self):
        # -----------------------------------------------------------------------
        # VALUE COUNT
        # -----------------------------------------------------------------------
        logger.info('\n' + '-' * 50)
        logger.info('value count')
        logger.info('-' * 50)

        # logger.info('value counts for {} column'.format(IRPC_COLUMN_NAME))
        # logger.info(self.csv_data[IRPC_COLUMN_NAME].value_counts(
        #     sort=True,
        #     # normalize=True
        # ))

    def _process_irpc(self):
        df = self.df
        logger.info(f'input df shape: {df.shape}')

        # This returns a pandas Series contain true for IRPC's which is not null
        null_rows = df.loc[~df["IRPC"].notnull()]
        logger.info(f'There are {len(null_rows)} rows which have null IRPC')
        self.null_irpc_count = len(null_rows)

        # Remove null IRPC's
        df = df.loc[df["IRPC"].notnull()]

        # convert type of IRPC to an integer instead of float
        logger.info(f"IRPC column has {df['IRPC'].dtypes} type")
        df["IRPC"] = df["IRPC"].astype(np.int64)

        logger.info(f'input df shape: {df.shape}')
        return df

    def _rename_cols(self):
        prefix = self.sheet_name + '.'
        columns = {d: prefix + d for d in list(self.df.columns) if d != 'IRPC'}
        self.renamed_cols = True
        logger.info(f'renamed and prefix added: {self.sheet_name}')
        return self.df.rename(columns=columns)

    def _process_date_fields(self):
        df = self.df
        if 'InterviewDate' in self.df:
            df.drop('InterviewDate', axis=1, inplace=True)
            self.removed_cols.append('InterviewDate')
            logger.info('InterviewDate removed')
        if 'BirthDate' in self.df:
            df.drop('BirthDate', axis=1, inplace=True)
            self.removed_cols.append('BirthDate')
            logger.info('InterviewDate removed')
            logger.info('BirthDate removed')
        return df

    def _remove_duplicate(self):
        logger.info('duplicated removed: --------')
        duplicated = self.df[self.df.duplicated(['IRPC'], keep=False)]
        logger.info(duplicated)
        self.duplicated_count = len(duplicated)
        return self.df[~self.df.duplicated(['IRPC'], keep='first')]
