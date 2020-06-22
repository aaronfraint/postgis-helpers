import requests
import zipfile
import io
from pathlib import Path

from .config_helpers import DEFAULT_DATA_INBOX, DEFAULT_DATA_OUTBOX


class DataSource():

    def __init__(self,
                 sql_table_name: str,
                 data_type: str,
                 url: str = None,
                 epsg: int = None,
                 import_folder: Path = None,
                 export_folder: Path = None,
                 zip_file: bool = False):

        self.SQL_TABLE_NAME = sql_table_name
        self.DATA_TYPE = data_type
        self.URL = url
        self.EPSG = epsg
        self.ZIP_FILE = zip_file

        # IMPORT FOLDER
        #   - This is where SQL is going to load data from
        #   - If the data has a URL, it will be downloaded into here
        #   - Otherwise, manually place the data into this folder
        if not import_folder:
            self.IMPORT_FOLDER = DEFAULT_DATA_INBOX / sql_table_name
        else:
            self.IMPORT_FOLDER = import_folder / "inbox" / sql_table_name

        # EXPORT FOLDER
        #   - Save here when exporting from SQL
        if not export_folder:
            self.EXPORT_FOLDER = DEFAULT_DATA_OUTBOX / sql_table_name
        else:
            self.EXPORT_FOLDER = export_folder / "outbox" / sql_table_name

    def filepath_import(self):
        """ Filepath that will be used to LOAD INTO SQL """

        return self.IMPORT_FOLDER / f"{self.SQL_TABLE_NAME}.{self.DATA_TYPE}"

    def filepath_export(self):
        """ Filepath for data EXPORTED FROM SQL """

        return self.EXPORT_FOLDER / f"{self.SQL_TABLE_NAME}.{self.DATA_TYPE}"

    def download_data(self, output_folder: Path = None):
        """ Download data from URL to the IMPORT_FOLDER """

        if not self.URL:
            print("This object does not have a URL")
            return None

        else:
            if not output_folder:
                output_folder = self.IMPORT_FOLDER

            if not output_folder.exists():
                output_folder.mkdir(parents=True)

            response = requests.get(self.URL)

            if self.ZIP_FILE:
                zipped_data = zipfile.ZipFile(io.BytesIO(response.content))
                zipped_data.extractall(output_folder)
            else:
                open_file = open(self.filepath_import(), 'wb')
                open_file.write(response.content)
                open_file.close()

    def flush_folder(self, data_folder: Path):
        """
        Delete all files within a folder,
        then delete the folder
        """

        if data_folder.exists():
            for f in data_folder.iterdir():
                if f.is_file():
                    f.unlink()
            data_folder.rmdir()
            print(f"Deleted {data_folder}")

    def flush_data(self):
        """ Delete the import and export folders """

        self.flush_folder(self.IMPORT_FOLDER)
        self.flush_folder(self.EXPORT_FOLDER)


csv_data = RawData(
    "covid_2020_06_10",
    "csv",
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/06-10-2020.csv"
)

shp_data = RawData(
    sql_table_name="high_injury_network_2017",
    data_type="shp",
    url="https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+high_injury_network_2017&filename=high_injury_network_2017&format=shp&skipfields=cartodb_id",
    epsg=2272,
    zip_file=True
)
