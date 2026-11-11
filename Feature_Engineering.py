import json
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import duckdb
from deltalake import write_deltalake


class NBAShotDataIngester:
    """
    Handles connection to Azure Data Lake Storage (ADLS Gen2)
    and ingests raw NBA shot chart JSON files into pandas DataFrames.
    """

    def __init__(self, account_name: str, sas_token: str, filesystem_name: str = "data"):
        self.account_name = account_name
        self.sas_token = sas_token
        self.filesystem_name = filesystem_name

    def _get_file_client(self, file_path: str):
        """Helper to initialize the ADLS file client."""
        account_url = self.account_name
        service_client = DataLakeServiceClient(account_url, credential=self.sas_token)
        return service_client.get_file_system_client(self.filesystem_name).get_file_client(file_path)

    def ingest_json_to_dataframe(self, file_path: str) -> pd.DataFrame:
        """
        Step 2: Ingests a raw JSON file from the Data Lake and parses
        the 'Shot_Chart_Detail' result set into a pandas DataFrame.
        """
        # 1. Download raw JSON file from Azure Data Lake Storage
        file_client = self._get_file_client(file_path)
        download_stream = file_client.download_file()
        json_bytes = download_stream.readall()

        # 2. Parse the JSON payload
        data = json.loads(json_bytes.decode('utf-8'))

        # 3. Extract the 'Shot_Chart_Detail' result set (Index 0 based on JSON schema)
        # Using safety check to locate 'Shot_Chart_Detail' dynamically if needed
        result_set = next(
            (rs for rs in data.get('resultSets', []) if rs['name'] == 'Shot_Chart_Detail'),
            data['resultSets'][0]
        )

        headers = result_set['headers']
        rows = result_set['rowSet']

        # 4. Return the extracted shooting data as a clean pandas DataFrame
        df = pd.DataFrame(rows, columns=headers)
        return df

    def extract_and_engineer_shots_with_duckdb(self, file_path: str) -> pd.DataFrame:
        """
        Ingests the raw JSON via DataFrame, then uses DuckDB to perform
        feature engineering (distances, angles, court types, and composite keys).
        """
        # 1. Get the raw DataFrame using our verified ingestion method
        df_raw = self.ingest_json_to_dataframe(file_path)

        # 2. Initialize DuckDB and query the DataFrame directly using column names
        con = duckdb.connect()

        query = """
        SELECT 
            GAME_ID,
            GAME_EVENT_ID,
            PLAYER_ID,
            PLAYER_NAME,
            PERIOD,
            (MINUTES_REMAINING * 60 + SECONDS_REMAINING) AS SECONDS_REMAINING_PERIOD,
            ACTION_TYPE,
            -- Map SHOT_TYPE: 3PT -> 3, else -> 2
            CASE WHEN SHOT_TYPE LIKE '%3PT%' THEN 3 ELSE 2 END AS SHOT_TYPE_NUM,
            LOC_X,
            LOC_Y,
            -- Calculate Shot Distance (in feet, converting NBA coordinate tenths of a foot)
            SQRT(POWER(LOC_X, 2) + POWER(LOC_Y, 2)) / 10.0 AS SHOT_DISTANCE_CALC,
            -- Calculate Shot Angle (in radians relative to basket at (0,0))
            ATAN2(LOC_Y, LOC_X) AS SHOT_ANGLE_RAD,
            CAST(SHOT_MADE_FLAG AS INTEGER) AS SHOT_MADE
        FROM df_raw
        WHERE LOC_X IS NOT NULL AND LOC_Y IS NOT NULL
        """

        # Execute query against the DataFrame
        df = con.execute(query).df()

        # 3. Set Composite Primary Key ('GAME_ID' + 'GAME_EVENT_ID') to match reference design
        df.set_index(["GAME_ID", "GAME_EVENT_ID"], inplace=True)

        return df

    def write_engineered_shots_to_delta(
            self,
            file_path: str,
            target_folder: str = "gold/delta_tables/",
            custom_table_name: str = None
    ) -> str:
        """
        Performs DuckDB feature engineering on a raw JSON file and saves the output
        as a Delta Lake table into a dedicated folder using write_deltalake.
        Checks for existing tables and defaults the name to '{original_name}_to_delta_lake'.
        """
        # 1. Extract the original file name without extension (e.g., '0022501165')
        original_name = file_path.split("/")[-1].replace(".json", "")

        # 2. Determine the Delta table name (default vs user-customized)
        table_name = custom_table_name if custom_table_name else f"{original_name}_to_delta_lake"

        # 3. Construct clean ABFSS paths
        base_account_name = self.account_name.replace("https://", "").replace(".dfs.core.windows.net", "").strip("/")
        cleaned_target_folder = target_folder.strip("/")
        delta_table_path = f"abfss://{self.filesystem_name}@{base_account_name}.dfs.core.windows.net/{cleaned_target_folder}/{table_name}/"

        # 4. Check if a Delta table already exists at the target path in ADLS
        service_client = DataLakeServiceClient(self.account_name, credential=self.sas_token)
        filesystem_client = service_client.get_file_system_client(self.filesystem_name)

        try:
            paths = filesystem_client.get_paths(path=f"{cleaned_target_folder}/{table_name}")
            exists = any(paths)
        except Exception:
            exists = False

        if exists:
            print(
                f"Notice: Delta table '{table_name}' already exists at path '{cleaned_target_folder}/'. Appending new records...")
        else:
            print(f"Creating new Delta table '{table_name}' at path '{cleaned_target_folder}/'...")

        # 5. Run feature engineering via DuckDB
        engineered_df = self.extract_and_engineer_shots_with_duckdb(file_path)

        # Reset index to convert primary keys back into regular columns for Delta storage
        df_to_write = engineered_df.reset_index()

        # 6. Configure Azure storage options for Delta Lake writer
        storage_options = {
            "azure_storage_account_name": base_account_name,
            "azure_storage_sas_key": self.sas_token
        }

        # 7. Write/Append data into Delta Lake format using write_deltalake
        write_deltalake(
            delta_table_path,
            df_to_write,
            mode="append",
            storage_options=storage_options,
            schema_mode="merge"
        )

        print(f"Successfully wrote Delta table to: {delta_table_path}")
        return delta_table_path

    def process_folder_to_delta(
            self,
            source_folder: str = "shotchart_json/202526_json/",
            target_folder: str = "shotchart_deltalake/202526_data/"
    ) -> list:
        """
        [PART TWO] Mass processes all JSON files in an ADLS source folder.
        Checks if each corresponding Delta table folder already exists in the target directory.
        If it exists, skips processing; otherwise, performs feature engineering
        and converts it to Delta Lake format.
        """
        service_client = DataLakeServiceClient(self.account_name, credential=self.sas_token)
        filesystem_client = service_client.get_file_system_client(self.filesystem_name)

        cleaned_source = source_folder.strip("/")
        cleaned_target = target_folder.strip("/")

        print(f"Scanning ADLS source folder: '{cleaned_source}/'...")

        # 1. List all paths in the source folder to find JSON files
        try:
            paths = filesystem_client.get_paths(path=cleaned_source)
            json_files = [p.name for p in paths if not p.is_directory and p.name.endswith(".json")]
        except Exception as e:
            print(f"Error accessing source folder '{cleaned_source}': {e}")
            return []

        if not json_files:
            print(f"No JSON files found in '{cleaned_source}/'.")
            return []

        print(f"Found {len(json_files)} JSON file(s). Checking Delta table existence in '{cleaned_target}/'...")
        processed_count = 0
        skipped_count = 0
        processed_paths = []

        # 2. Iterate through each JSON file
        for file_path in json_files:
            original_name = file_path.split("/")[-1].replace(".json", "")
            table_name = f"{original_name}_to_delta_lake"
            delta_check_path = f"{cleaned_target}/{table_name}"

            # Check if the Delta table folder already exists in ADLS
            try:
                existing_paths = filesystem_client.get_paths(path=delta_check_path)
                exists = any(existing_paths)
            except Exception:
                exists = False

            # 3. Skip if already created, otherwise process using your Part One method
            if exists:
                print(f"  [SKIPPED] Delta table '{table_name}' already exists.")
                skipped_count += 1
                continue

            print(f"  [PROCESSING] '{file_path}' -> Delta table '{table_name}'...")
            try:
                # Reusing your robust single-file method
                delta_path = self.write_engineered_shots_to_delta(
                    file_path=file_path,
                    target_folder=cleaned_target,
                    custom_table_name=table_name
                )
                processed_paths.append(delta_path)
                processed_count += 1
            except Exception as err:
                print(f"  [ERROR] Failed to process {file_path}: {err}")

        print(f"\nBatch Processing Complete! Processed: {processed_count} | Skipped (Already Existed): {skipped_count}")
        return processed_paths

