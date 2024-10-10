import os
import sys

from pandas import DataFrame

from constants.idColumns import ID_COLUMNS
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.TableNames import TableNames
from utils.file_utils import read_tabular_file_as_string

if __name__ == '__main__':
    sample_columns = [MetadataColumns.normalize_name(column_name="SampleBarcode"),
                      MetadataColumns.normalize_name(column_name="Sampling"),
                      MetadataColumns.normalize_name(column_name="SampleQuality"),
                      MetadataColumns.normalize_name(column_name="SamTimeCollected"),
                      MetadataColumns.normalize_name(column_name="SamTimeReceived"),
                      MetadataColumns.normalize_name(column_name="TooYoung"),
                      MetadataColumns.normalize_name(column_name="BIS")]

    folder = sys.argv[1]
    data_filepath = sys.argv[2]
    df = read_tabular_file_as_string(filepath=f"{os.path.join(folder, data_filepath)}", read_as_string=False)
    df.rename(columns=lambda x: MetadataColumns.normalize_name(column_name=x), inplace=True)

    # get sample data: all sample columns + patient id
    sample_columns_and_patient_column = []
    sample_columns_and_patient_column.extend(sample_columns)
    sample_columns_and_patient_column.append(ID_COLUMNS[HospitalNames.IT_BUZZI_UC1][TableNames.PATIENT])
    df_samples = df[sample_columns_and_patient_column]

    # get lab data: no sample column, except sampleBarcode
    sample_columns_without_barcode = sample_columns
    sample_columns_without_barcode.remove(MetadataColumns.normalize_name(column_name="SampleBarcode"))
    df_lab = DataFrame(df)
    df_lab.drop(sample_columns_without_barcode, axis=1, inplace=True)

    df_lab.to_csv(f"{os.path.join(folder, "screening-laboratory.csv")}", index=False)
    df_samples.to_csv(f"{os.path.join(folder, "screening-sample.csv")}", index=False)
