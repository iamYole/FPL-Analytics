import requests
import os
import re
import logging
import json
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Configure logging to output to the console
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# URL = os.getenv('API_URL')

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv('HOST', 'localhost'),   # fallback to localhost
            port=os.getenv('PORT', 5432),
            database=os.getenv('DATABASE'),
            user=os.getenv('USERNAME'),
            password=os.getenv('PASSWORD')
        )
        return conn
    except Exception as e:
        print(f"❌ Error connecting to Postgres: {e}")

def extract_player_info(league_id, conn):
    all_results = []
    page = 1
    has_next = True
    while has_next:
        league_URL = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/?page_standings={page}"
        try:
            response = requests.get(league_URL)
            response.raise_for_status()  # ✅ fail fast if API request fails
            data =  response.json()

            results = data.get("standings", {}).get("results",[])
            all_results.extend(results)

            has_next = data.get("standings", {}).get("has_next", False)
            page += 1
        except Exception as e:
            logger.error(f"Error getting player details: {e}")
            break

    if all_results is None:
        logger.warning("No data found")
    else:
        try:
            cursor = conn.cursor()
            qry = """
                    INSERT INTO fpl.player_details (team_id,player_name,team_name,current_rank,
                                            previous_rank,rank_sort,total_point)
                                    VALUES(%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (team_id) DO UPDATE
                    SET team_name = EXCLUDED.team_name
                    WHERE fpl.player_details.team_name IS DISTINCT FROM EXCLUDED.team_name;;
                    """
            players_data = [(row["entry"],row["player_name"],row["entry_name"],row["rank"],
                        row["last_rank"],row["rank_sort"],row["event_total"] ) for row in all_results]
            cursor.executemany(qry,players_data)
            conn.commit()
            logger.info("Players detais downloaded")
        except Exception as e:
            logger.error(f"Error saving player details {e}")
        # Save to JSON
        # with open("player_details.json", "w") as f:
        #     json.dump(all_results, f, indent=4)


    # Extract all the IDs
    player_ids = [item['entry'] for item in all_results]
    return player_ids

def extract_league_data(league_id,conn):
    league_URL = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"

    try:
        response = requests.get(league_URL)
        response.raise_for_status()
        data = response.json()['league']

        if not data:
            logger.warning(f"No data found for league: {league_id}")

        # with open("league_details.json", "w") as f:
        #     json.dump(data, f, indent=4)

        try:
            cursor = conn.cursor()
            qry = """
                INSERT INTO fpl.league_details(league_id,league_name,created_date)
		                VALUES(%s,%s,%s)
                ON CONFLICT (league_id) DO NOTHING;
            """
            lid = data["id"]
            league_name = re.sub(r'[^\x00-\x7F]+','', data['name']) 
            created_date = data["created"]
            
            cursor.execute(qry,(lid,league_name,created_date))
            conn.commit()

            logger.info("league data saved successfully")
        except Exception as e:
            logger.error(f"Error saving league data {e}")

    except Exception as e:
        logger.error(f"Error downloading league data {e}")


def extract_gw_data(league_id,player_ids,conn):
    all_data = {}
    for player in player_ids:
        game_week_URL = f"https://fantasy.premierleague.com/api/entry/{player}/history"
        try:
            response = requests.get(game_week_URL)
            response.raise_for_status()  # ✅ fail fast if API request fails
            data = response.json().get('current', [])

            if not data:
                logger.warning(f"No data found for {player}")
                continue

            all_data[player] = data
            # logger.info(f"Extracted data for player {player}")
        except Exception as e:
            logger.error(f"Error downloading game week data for player: {player} {e}")

    try:
        gw_data = []
        cursor = conn.cursor()
        qry = """
            INSERT INTO fpl.gw_events (game_week,weeks_points,total_points,bank,transfers,transfer_cost,gross_points,
							bench_points,team_id,league_id,overall_rank, team_value)
		            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (game_week,team_id,league_id) DO NOTHING;
            """
        for player_id, results in all_data.items():
            for result in results:
                gross_point = result["points"] + result["event_transfers_cost"]
                values = (
                    result["event"],
                    result["points"],
                    result["total_points"],
                    result["bank"],
                    result["event_transfers"],
                    result["event_transfers_cost"],
                    gross_point,
                    result["points_on_bench"],
                    player_id,
                    int(league_id),
                    result["overall_rank"],
                    result['value']
                )
                # print(values)
            cursor.execute(qry, values)
        conn.commit()
        logger.info("Gameweek data saved successfully")       
    except Exception as e:
        logger.error(f"Error saving game week data {e}")
    # Save to JSON file
    # with open("gameweek_data.json", "w") as f:
    #     json.dump(all_data, f, indent=4)  # indent=4 for readability
    # logger.info(f"Gameweek data saved successfully!")


def main():
    conn = connect_to_db()
    league_id = os.getenv("LEAGUE_ID")
    extract_league_data(league_id=league_id, conn=conn)
    player_ids=extract_player_info(league_id=league_id, conn=conn)
    # print(len(player_ids))
    extract_gw_data(league_id,player_ids, conn=conn)
   


if __name__ == "__main__":
    main()