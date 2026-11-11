import time
import pandas as pd
import requests
import time
import random
import curl_cffi.requests as ccr
import pathlib
import pyarrow


# 1. Custom HTTP Headers (Required to avoid HTTP 403 Forbidden / Cloudflare blocks)
HEADERS = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true',
    # Modern Client Hints required by Akamai edge filters:
    'Sec-Ch-Ua': '"Chromium";v="128", "Not=A?Brand";v="24", "Google Chrome";v="128"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Origin': 'https://www.nba.com',
    'Referer': 'https://www.nba.com/',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
}

def get_season_game_ids_regular_season(season="2025-26", header=None):
    if header is None:
        header = HEADERS

    url = "https://stats.nba.com/stats/leaguegamefinder"
    params = {
        'LeagueID': '00',
        'Season': season,
        'SeasonType': 'Regular Season',
        'PlayerOrTeam': 'T'  # Explicitly query team games to get game logs
    }

    # Perform GET request with TLS impersonation via ccr
    response = ccr.get(
        url,
        headers=header,
        params=params,
        impersonate="chrome120",
        timeout=15
    )

    if response.status_code != 200:
        raise Exception(f"Failed to fetch games. Status code: {response.status_code}")

    data = response.json()

    # Extract headers and row values from the JSON structure
    result_set = data['resultSets'][0]
    headers = result_set['headers']
    rows = result_set['rowSet']

    df_games = pd.DataFrame(rows, columns=headers)

    # Deduplicate GameIDs (each game has two entries: one for home team, one for away)
    game_ids = df_games['GAME_ID'].unique().tolist()
    return game_ids



def fetch_game_shot_chart(game_id, season="2025-26", header=None):
    if header is None:
        header = HEADERS
    url = "https://stats.nba.com/stats/shotchartdetail"

    params = {
        'AheadBehind': '',
        'CFID': '',
        'CFPARAMS': '',
        'ClutchTime': '',
        'Conference': '',
        'ContextFilter': '',
        'ContextMeasure': 'FGA',
        'DateFrom': '',
        'DateTo': '',
        'Division': '',
        'EndPeriod': '0',
        'EndRange': '28800',
        'GROUP_ID': '',
        'GameEventID': '',
        'GameID': game_id,
        'GameSegment': '',
        'GroupID': '',
        'GroupMode': '',
        'GroupQuantity': '5',
        'LastNGames': '0',
        'LeagueID': '00',
        'Location': '',
        'Month': '0',
        'OnOff': '',
        'OppPlayerID': '',
        'OpponentTeamID': '0',
        'Outcome': '',
        'PORound': '0',
        'Period': '0',
        'PlayerID': '0',  # 0 returns ALL players
        'PlayerID1': '',
        'PlayerID2': '',
        'PlayerID3': '',
        'PlayerID4': '',
        'PlayerID5': '',
        'PlayerPosition': '',
        'PointDiff': '',
        'Position': '',
        'RangeType': '0',
        'RookieYear': '',
        'Season': season,
        'SeasonSegment': '',
        'SeasonType': 'Regular Season',
        'ShotClockRange': '',
        'StartPeriod': '0',
        'StartRange': '0',
        'StarterBench': '',
        'TeamID': '0',  # 0 returns BOTH teams
        'VsConference': '',
        'VsDivision': '',
        'VsPlayerID1': '',
        'VsPlayerID2': '',
        'VsPlayerID3': '',
        'VsPlayerID4': '',
        'VsPlayerID5': '',
        'VsTeamID': ''
    }

    # Perform GET request using curl_cffi with Chrome TLS impersonation
    response = ccr.get(
        url,
        headers=header,
        params=params,
        impersonate="chrome120",
        timeout=15
    )

    if response.status_code != 200:
        raise Exception(f"Failed to fetch shot chart for GameID {game_id}. Status code: {response.status_code}")

    data = response.json()

    # Extract the Shot_Chart_Detail result set (Index 0)
    result_set = data['resultSets'][0]
    headers = result_set['headers']
    rows = result_set['rowSet']

    return pd.DataFrame(rows, columns=headers)
# #it returns a panda data frame

def fetch_game_shot_chart_json(game_id, season="2025-26", header=None):
    if header is None:
        header = HEADERS
    url = "https://stats.nba.com/stats/shotchartdetail"

    params = {
        'AheadBehind': '',
        'CFID': '',
        'CFPARAMS': '',
        'ClutchTime': '',
        'Conference': '',
        'ContextFilter': '',
        'ContextMeasure': 'FGA',
        'DateFrom': '',
        'DateTo': '',
        'Division': '',
        'EndPeriod': '0',
        'EndRange': '28800',
        'GROUP_ID': '',
        'GameEventID': '',
        'GameID': game_id,
        'GameSegment': '',
        'GroupID': '',
        'GroupMode': '',
        'GroupQuantity': '5',
        'LastNGames': '0',
        'LeagueID': '00',
        'Location': '',
        'Month': '0',
        'OnOff': '',
        'OppPlayerID': '',
        'OpponentTeamID': '0',
        'Outcome': '',
        'PORound': '0',
        'Period': '0',
        'PlayerID': '0',  # 0 returns ALL players
        'PlayerID1': '',
        'PlayerID2': '',
        'PlayerID3': '',
        'PlayerID4': '',
        'PlayerID5': '',
        'PlayerPosition': '',
        'PointDiff': '',
        'Position': '',
        'RangeType': '0',
        'RookieYear': '',
        'Season': season,
        'SeasonSegment': '',
        'SeasonType': 'Regular Season',
        'ShotClockRange': '',
        'StartPeriod': '0',
        'StartRange': '0',
        'StarterBench': '',
        'TeamID': '0',  # 0 returns BOTH teams
        'VsConference': '',
        'VsDivision': '',
        'VsPlayerID1': '',
        'VsPlayerID2': '',
        'VsPlayerID3': '',
        'VsPlayerID4': '',
        'VsPlayerID5': '',
        'VsTeamID': ''
    }

    # Perform GET request using curl_cffi with Chrome TLS impersonation
    response = ccr.get(
        url,
        headers=header,
        params=params,
        impersonate="chrome120",
        timeout=15
    )

    if response.status_code != 200:
        raise Exception(f"Failed to fetch shot chart for GameID {game_id}. Status code: {response.status_code}")

    return response.json()

# creating a csv file for later retrival of files
# csv file:
# row_id: index starting from 0
# game_id: id of games of a season
# status: default as PENDING to indicate it is not fetched
# parameters:
# season_games_ids = a list of id
# path: the path you want your file to be in
# csv_name = "season_id": the name of the csv file
# status: indicating whether it is retrieved or not
# for the path. remember to use the format" r"C:\..."
# a folder would be created when the dedicated data is not created yet
def season_game_ids_column_tocsv(season_game_ids,path,csv_name = "season_id"):
    df = pd.DataFrame(season_game_ids,columns=["game_id"])
    df["status"] = "PENDING"

    output_path = pathlib.Path(path)
    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / (csv_name + ".csv")

    df.to_csv(file_path, index_label="row_id")
    print(f"csv saved to {file_path}")

"""
when you have created the csv file, you can use this to retrieve all shot charts for every game for a season 
This is for retrieving parquet files into your local disk. If you want to put it in an Azure Data Lake, you should check out Azure_Data_Lake.py

parameters:
csv_file_path: to indicate the path for the csv file with the format r"C:\\...\\{csv file name}
output_file_path: path where the csv files of the short charts are stored
season: season of that batch
time_interval=1: to indicate the stopping time between each retrieval to prevent exceeding the rate limit
indicator="": a set of words that would be added at the front of every short chart csv file, primarily for indicating which season it belongs to 
h = HEADER: header of the request
"""
def fetch_game_shot_chart_per_season(csv_file_path, output_file_path, season, time_interval=1, indicator = "", h=None):
    if h is None:
        h = HEADERS
    csv_path = pathlib.Path(csv_file_path)
    shots_dir = pathlib.Path(output_file_path)
    shots_dir.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        raise FileNotFoundError(f"Tracking CSV not found at: {csv_path}")

    # Ensure GAME_ID is read as string to preserve leading zeros (e.g. '0022300001')
    df_tracking = pd.read_csv(csv_path, dtype={'game_id': str})

    # Filter for games that still need processing
    pending_mask = df_tracking['status'].fillna('PENDING') == 'PENDING'
    pending_indices = df_tracking[pending_mask].index.tolist()

    print(f"Total Games in Tracker: {len(df_tracking)}")
    print(f"Remaining Games to Fetch: {len(pending_indices)}")

    total_games = len(df_tracking)
    remaining_games = len(pending_indices)

    if remaining_games == 0:
        print("all files retrieved")
        return 0

    for count,idx in enumerate(pending_indices,start=1):
        row_id = df_tracking['row_id'].iloc[idx]
        game_id = df_tracking['game_id'].iloc[idx]
        output_file = shots_dir / f"{indicator}_{game_id}.parquet"

        if output_file.exists():
            df_tracking.at[idx, 'status'] = 'FETCHED'
            df_tracking.to_csv(csv_path, index=False)
            print(
                f"[{count}/{remaining_games}] row_id={row_id} | GameID {game_id} exists locally. Status updated to FETCHED.")
            continue

        try:
            # Sleep interval with slight jitter to respect NBA API rate limits
            time.sleep(time_interval + random.uniform(0.5, 1.5))

            # Query shot chart endpoint
            df_shots = fetch_game_shot_chart(game_id, season)
            # Store shot data in Parquet format
            df_shots.to_parquet(output_file, index=False,compression='snappy')

            # Mark status as FETCHED upon success
            df_tracking.at[idx, 'status'] = 'FETCHED'
            df_tracking.to_csv(csv_path, index=False)
            print(f"shot chart for {game_id} has been fetched")

        except Exception as e:
            print(f"  failed for GameID {game_id}: {e}")
            return "FAILED RETRIEVAL"

    return "all data has been retrieved"






