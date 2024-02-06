"""This module is for data validations"""


def mandatoryComparision(source_df, metadata_df):
    """This method is to get the list of columns with no data """
    try:
        empty_columns = []
        metadata_columns = metadata_df.index
        for column in metadata_columns:
            m_field = metadata_df.loc[column, 'mandatory']
            if column in source_df:
                missing_rows = source_df[column].isnull().sum()
                if m_field and missing_rows > 0:
                    empty_columns.append(column)
        print(empty_columns)
        return empty_columns
    except Exception as error:
        print(error)
        return error


def checkingMandatoryColumns(source_df, metadata_df):
    try:
        missing_mandatory_columns = []
        source_df_columns = [column for column in source_df]
        metadata_columns = metadata_df.index
        for column in metadata_columns:
            column_mandatory_value = metadata_df.loc[column, 'column_mandatory']
            print("status", column_mandatory_value)
            if column_mandatory_value and column not in source_df_columns:
                missing_mandatory_columns.append(column)
        return missing_mandatory_columns
    except Exception as error:
        print(error)
        return error
