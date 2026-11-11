import io
import os
import time
import random
import pathlib
import pandas as pd
import curl_cffi.requests as ccr
from azure.storage.filedatalake import DataLakeServiceClient
from Data_Extraction_Shot import HEADERS
from Data_Extraction_Shot import fetch_game_shot_chart, get_season_game_ids_regular_season, fetch_game_shot_chart_json
import json


class AzureDataLakeManager:
    """
    A manager class to handle interactions with Azure Data Lake Storage Gen2,
    allowing dynamic configuration of account URL and SAS tokens for client usage.
    """

    def __init__(self, account_url: str = None , sas_token: str = None):
        """
        Initializes the manager with the target Azure Data Lake account URL and credentials.
        Falls back to environment variables if parameters are omitted.
        """
        self.account_url = account_url
        self.sas_token = os.getenv("AZURE_SAS_TOKEN", sas_token)

        # print(account_url)
        # print(sas_token)

        if (self.account_url is None) or (self.sas_token is None):
            raise ValueError(
                "A valid SAS token must be provided either via initialization parameters "
                "or the 'AZURE_SAS_TOKEN' environment variable."
            )

    def get_adls_file_client(self, container_name: str, file_path: str):
        """Instantiates DataLakeServiceClient using the instance's credentials."""
        service_client = DataLakeServiceClient(account_url=self.account_url, credential=self.sas_token)
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        return file_system_client.get_file_client(file_path)






    # debugging purposes, for showing all paths in the container
    def _find_paths_in_container(self, container_name: str):
        """Instantiates DataLakeServiceClient using the instance's credentials."""
        service_client = DataLakeServiceClient(account_url = self.account_url, credential=self.sas_token)
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        paths = file_system_client.get_paths()
        print(paths)
        for path in paths:
            print(path)
        print("all paths displayed")
        return file_system_client.get_paths()





    def fetch_game_shot_chart_per_season_adls_parquet(
            self,
            container_name: str,
            adls_csv_path: str,
            adls_output_folder: str,
            season: str,
            time_interval: float = 1.0,
            indicator: str = "",
            h: dict = None
    ):
        """
        Reads the tracking CSV directly from Azure Data Lake Storage, fetches missing
        shot charts, writes Parquet files to ADLS, and updates the tracking CSV in ADLS.
        """
        if h is None:
            h = HEADERS

        # 1. Initialize ADLS File Client for the tracking CSV
        csv_file_client = self.get_adls_file_client(container_name, adls_csv_path)

        if not csv_file_client.exists():
            raise FileNotFoundError(
                f"Tracking CSV not found in ADLS container '{container_name}' at path: {adls_csv_path}")

        # 2. Read tracking CSV directly from ADLS stream into Pandas DataFrame
        downloaded_bytes = csv_file_client.download_file().readall()
        df_tracking = pd.read_csv(io.BytesIO(downloaded_bytes), dtype={'game_id': str})

        # 3. Identify remaining PENDING rows
        pending_mask = df_tracking['status'].fillna('PENDING') == 'PENDING'
        pending_indices = df_tracking[pending_mask].index.tolist()

        total_games = len(df_tracking)
        remaining_games = len(pending_indices)

        print(f"Total Games in ADLS Tracker: {total_games}")
        print(f"Remaining Games to Fetch: {remaining_games}")

        if remaining_games == 0:
            print("All files have already been retrieved!")
            return "ALL RETRIEVED"

        # Helper function to persist updated CSV back to ADLS
        def save_csv_to_adls(df: pd.DataFrame):
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_file_client.upload_data(csv_buffer.getvalue().encode('utf-8'), overwrite=True)

        for count, idx in enumerate(pending_indices, start=1):
            row_id = df_tracking.at[idx, 'row_id'] if 'row_id' in df_tracking.columns else idx
            game_id = str(df_tracking.at[idx, 'game_id']).zfill(10)

            # Build Parquet target path in ADLS
            remote_filename = f"{indicator}_{game_id}.parquet" if indicator else f"{game_id}.parquet"
            adls_parquet_path = f"{adls_output_folder.strip('/')}/{remote_filename}"

            parquet_file_client = self.get_adls_file_client(container_name, adls_parquet_path)

            # Skip API call if Parquet file already exists in ADLS
            if parquet_file_client.exists():
                df_tracking.at[idx, 'status'] = 'FETCHED'
                save_csv_to_adls(df_tracking)
                print(
                    f"[{count}/{remaining_games}] row_id={row_id} | GameID {game_id} exists in ADLS. Status updated to FETCHED.")
                continue

            print(f"[{count}/{remaining_games}] Fetching row_id={row_id} | GameID: {game_id}...")

            try:
                # Sleep with jitter to respect rate limits
                time.sleep(time_interval + random.uniform(0.5, 1.5))

                # Query stats.nba.com
                df_shots = fetch_game_shot_chart(game_id, season=season, header=h)
                print("successfully fetched shot chart.")

                # Convert DataFrame to Parquet byte stream in memory
                parquet_buffer = io.BytesIO()
                df_shots.to_parquet(parquet_buffer, index=False, compression='snappy')
                print("successful conversion to parquet.")

                # Upload Parquet payload directly to ADLS
                parquet_file_client.upload_data(parquet_buffer.getvalue(), overwrite=True)
                print("successfully uploaded shot chart.")

                # Update in-memory state and push updated tracking CSV to ADLS
                df_tracking.at[idx, 'status'] = 'FETCHED'
                save_csv_to_adls(df_tracking)
                print(f"  └─ Shot chart for {game_id} uploaded to ADLS.")
                print("successfully updated tracking CSV.")

            except Exception as e:
                print(f"  └─ Request failed for GameID {game_id}: {e}")
                print("Process halted. Status remains PENDING in ADLS for subsequent re-runs.")
                return "FAILED RETRIEVAL"

        return "All data retrieved and stored in ADLS!"




    def fetch_game_shot_chart_per_season_adls_json(
            self,
            container_name: str,
            adls_csv_path: str,
            adls_output_folder: str,
            season: str,
            time_interval: float = 1.0,
            indicator: str = "",
            h: dict = None
    ):
        """
        Reads the tracking CSV directly from Azure Data Lake Storage, fetches missing
        shot charts as JSON payloads, writes JSON files to ADLS, and updates the tracking CSV in ADLS.
        """
        if h is None:
            h = HEADERS

        # 1. Initialize ADLS File Client for the tracking CSV
        csv_file_client = self.get_adls_file_client(container_name, adls_csv_path)

        if not csv_file_client.exists():
            raise FileNotFoundError(
                f"Tracking CSV not found in ADLS container '{container_name}' at path: {adls_csv_path}")

        # 2. Read tracking CSV directly from ADLS stream into Pandas DataFrame
        downloaded_bytes = csv_file_client.download_file().readall()
        df_tracking = pd.read_csv(io.BytesIO(downloaded_bytes), dtype={'game_id': str})

        # 3. Identify remaining PENDING rows
        pending_mask = df_tracking['status'].fillna('PENDING') == 'PENDING'
        pending_indices = df_tracking[pending_mask].index.tolist()

        total_games = len(df_tracking)
        remaining_games = len(pending_indices)

        print(f"Total Games in ADLS Tracker: {total_games}")
        print(f"Remaining Games to Fetch: {remaining_games}")

        if remaining_games == 0:
            print("All JSON files have already been retrieved!")
            return "ALL RETRIEVED"

        # Helper function to persist updated CSV back to ADLS
        def save_csv_to_adls(df: pd.DataFrame):
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_file_client.upload_data(csv_buffer.getvalue().encode('utf-8'), overwrite=True)

        for count, idx in enumerate(pending_indices, start=1):
            row_id = df_tracking.at[idx, 'row_id'] if 'row_id' in df_tracking.columns else idx
            game_id = str(df_tracking.at[idx, 'game_id']).zfill(10)

            # Build JSON target path in ADLS
            remote_filename = f"{indicator}_{game_id}.json" if indicator else f"{game_id}.json"
            adls_json_path = f"{adls_output_folder.strip('/')}/{remote_filename}"

            json_file_client = self.get_adls_file_client(container_name, adls_json_path)

            # Skip API call if JSON file already exists in ADLS
            if json_file_client.exists():
                df_tracking.at[idx, 'status'] = 'FETCHED'
                save_csv_to_adls(df_tracking)
                print(
                    f"[{count}/{remaining_games}] row_id={row_id} | GameID {game_id} JSON exists in ADLS. Status updated to FETCHED.")
                continue

            print(f"[{count}/{remaining_games}] Fetching JSON shot chart for row_id={row_id} | GameID: {game_id}...")

            try:
                # Sleep with jitter to respect rate limits
                time.sleep(time_interval + random.uniform(0.5, 1.5))

                # Query stats.nba.com for the raw JSON response
                json_data = fetch_game_shot_chart_json(game_id, season=season, header=h)
                print("successfully fetched shot chart JSON.")

                # Convert dictionary to JSON byte stream in memory
                json_bytes = json.dumps(json_data).encode('utf-8')
                print("successful conversion to JSON bytes.")

                # Upload JSON payload directly to ADLS
                json_file_client.upload_data(json_bytes, overwrite=True)
                print("successfully uploaded JSON shot chart.")

                # Update in-memory state and push updated tracking CSV to ADLS
                df_tracking.at[idx, 'status'] = 'FETCHED'
                save_csv_to_adls(df_tracking)
                print(f"  └─ Shot chart JSON for {game_id} uploaded to ADLS.")
                print("successfully updated tracking CSV.")

            except Exception as e:
                print(f"  └─ Request failed for GameID {game_id}: {e}")
                print("Process halted. Status remains PENDING in ADLS for subsequent re-runs.")
                return "FAILED RETRIEVAL"

        return "All JSON data retrieved and stored in ADLS!"

