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

def element_types(conn):
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()['element_types']

        if data is None:
            logger.warning("No data found")

        try:
            cursor = conn.cursor()
            qry = """
                INSERT INTO fpl.elements_type (
                    id, singular_name, singular_name_short, element_count
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    singular_name = EXCLUDED.singular_name,
                    singular_name_short = EXCLUDED.singular_name_short,
                    element_count = EXCLUDED.element_count;
            """

            for element_type in data: 
                cursor.execute(qry, (
                    element_type.get("id"),
                    element_type.get("singular_name"),
                    element_type.get("singular_name_short"),
                    element_type.get("element_count")
                ))

            conn.commit()
            logger.info("Element types saved successfully")

        except Exception as e:
            logger.error(f"Error saving element types: {e}")


    except Exception as e:
        logger.error(f"Error {e}")

def extract_teams(conn):
    """ Extract teams"""
    team_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    try:
        response = requests.get(team_url)
        response.raise_for_status()
        data = response.json()['teams']

        if data is None:
            logger.warning("No team data found")

        try:
            cursor = conn.cursor()
            qry = """
                INSERT into fpl.teams (team_code,team_name,short_name,strength,strength_overall_home,
						strength_overall_away,strength_attack_home,strength_attack_away,
						strength_defence_home,strength_defence_away,wins,draws,loss,played)
			        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (team_code) DO UPdATE SET
                    strength = EXCLUDED.strength,
                    strength_overall_home = EXCLUDED.strength_overall_home,
                    strength_overall_away = EXCLUDED.strength_overall_away,
                    strength_attack_home = EXCLUDED.strength_attack_home,
                    strength_attack_away = EXCLUDED.strength_attack_away,
                    strength_defence_home = EXCLUDED.strength_defence_home,
                    strength_defence_away = EXCLUDED.strength_defence_away,
                    wins = EXCLUDED.wins,
                    draws = EXCLUDED.draws,
                    loss = EXCLUDED.loss;
            """
            
            for team in data:
                cursor.execute(qry,
                               (
                                   team["code"],team["name"],team["short_name"],team["strength"],team["strength_overall_home"],
                                team["strength_overall_away"],team["strength_attack_home"],team["strength_attack_away"],team["strength_defence_home"],
                                team["strength_defence_away"], team["win"],team["draw"],team["loss"],team["played"]
                               ))
            conn.commit()

            logger.info("Team data saved successfully")
        except Exception as e:
            logger.error(f"Error saving Team data {e}")

        # with open("team.json", "w") as f:
        #     json.dump(data, f, indent=4)
    except Exception as e:
        logger.error(f"Error downloading league data {e}")


def extract_elements(conn):
    """Extract the football players data"""
    team_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    try:
        response = requests.get(team_url)
        response.raise_for_status()
        data = response.json()['elements']

        if data is None:
            logger.warning("No Element data found")

        try:
            cursor = conn.cursor()
            qry = """
                INSERT INTO fpl.elements_details (
                    id, code, dreamteam_count, element_type, ep_next, ep_this, event_points,
                    first_name, form, in_dreamteam, now_cost, points_per_game, second_name,
                    selected_by_percent, special, team, team_code, total_points, transfers_in,
                    transfers_in_event, transfers_out, transfers_out_event, value_form, value_season,
                    web_name, region, team_join_date, birth_date, has_temporary_code, minutes,
                    goals_scored, assists, clean_sheets, goals_conceded, own_goals, penalties_saved,
                    penalties_missed, yellow_cards, red_cards, saves, bonus, bps, influence,
                    creativity, threat, ict_index, clearances_blocks_interceptions, recoveries, tackles,
                    defensive_contribution, starts, expected_goals, expected_assists, expected_goal_involvements,
                    expected_goals_conceded, influence_rank, influence_rank_type, creativity_rank,
                    creativity_rank_type, threat_rank, threat_rank_type, ict_index_rank, ict_index_rank_type,
                    corners_and_indirect_freekicks_order, corners_and_indirect_freekicks_text,
                    direct_freekicks_order, direct_freekicks_text, penalties_order, penalties_text,
                    expected_goals_per_90, saves_per_90, expected_assists_per_90,
                    expected_goal_involvements_per_90, expected_goals_conceded_per_90, goals_conceded_per_90,
                    now_cost_rank, now_cost_rank_type, form_rank, form_rank_type,
                    points_per_game_rank, points_per_game_rank_type, selected_rank, selected_rank_type,
                    starts_per_90, clean_sheets_per_90, defensive_contribution_per_90
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,
                    %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,
                    %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,
                    %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,
                    %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,
                    %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,
                    %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,
                    %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,
                    %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    code = EXCLUDED.code,
                    dreamteam_count = EXCLUDED.dreamteam_count,
                    element_type = EXCLUDED.element_type,
                    ep_next = EXCLUDED.ep_next,
                    ep_this = EXCLUDED.ep_this,
                    event_points = EXCLUDED.event_points,
                    first_name = EXCLUDED.first_name,
                    form = EXCLUDED.form,
                    in_dreamteam = EXCLUDED.in_dreamteam,
                    now_cost = EXCLUDED.now_cost,
                    points_per_game = EXCLUDED.points_per_game,
                    second_name = EXCLUDED.second_name,
                    selected_by_percent = EXCLUDED.selected_by_percent,
                    special = EXCLUDED.special,
                    team = EXCLUDED.team,
                    team_code = EXCLUDED.team_code,
                    total_points = EXCLUDED.total_points,
                    transfers_in = EXCLUDED.transfers_in,
                    transfers_in_event = EXCLUDED.transfers_in_event,
                    transfers_out = EXCLUDED.transfers_out,
                    transfers_out_event = EXCLUDED.transfers_out_event,
                    value_form = EXCLUDED.value_form,
                    value_season = EXCLUDED.value_season,
                    web_name = EXCLUDED.web_name,
                    region = EXCLUDED.region,
                    team_join_date = EXCLUDED.team_join_date,
                    birth_date = EXCLUDED.birth_date,
                    has_temporary_code = EXCLUDED.has_temporary_code,
                    minutes = EXCLUDED.minutes,
                    goals_scored = EXCLUDED.goals_scored,
                    assists = EXCLUDED.assists,
                    clean_sheets = EXCLUDED.clean_sheets,
                    goals_conceded = EXCLUDED.goals_conceded,
                    own_goals = EXCLUDED.own_goals,
                    penalties_saved = EXCLUDED.penalties_saved,
                    penalties_missed = EXCLUDED.penalties_missed,
                    yellow_cards = EXCLUDED.yellow_cards,
                    red_cards = EXCLUDED.red_cards,
                    saves = EXCLUDED.saves,
                    bonus = EXCLUDED.bonus,
                    bps = EXCLUDED.bps,
                    influence = EXCLUDED.influence,
                    creativity = EXCLUDED.creativity,
                    threat = EXCLUDED.threat,
                    ict_index = EXCLUDED.ict_index,
                    clearances_blocks_interceptions = EXCLUDED.clearances_blocks_interceptions,
                    recoveries = EXCLUDED.recoveries,
                    tackles = EXCLUDED.tackles,
                    defensive_contribution = EXCLUDED.defensive_contribution,
                    starts = EXCLUDED.starts,
                    expected_goals = EXCLUDED.expected_goals,
                    expected_assists = EXCLUDED.expected_assists,
                    expected_goal_involvements = EXCLUDED.expected_goal_involvements,
                    expected_goals_conceded = EXCLUDED.expected_goals_conceded,
                    influence_rank = EXCLUDED.influence_rank,
                    influence_rank_type = EXCLUDED.influence_rank_type,
                    creativity_rank = EXCLUDED.creativity_rank,
                    creativity_rank_type = EXCLUDED.creativity_rank_type,
                    threat_rank = EXCLUDED.threat_rank,
                    threat_rank_type = EXCLUDED.threat_rank_type,
                    ict_index_rank = EXCLUDED.ict_index_rank,
                    ict_index_rank_type = EXCLUDED.ict_index_rank_type,
                    corners_and_indirect_freekicks_order = EXCLUDED.corners_and_indirect_freekicks_order,
                    corners_and_indirect_freekicks_text = EXCLUDED.corners_and_indirect_freekicks_text,
                    direct_freekicks_order = EXCLUDED.direct_freekicks_order,
                    direct_freekicks_text = EXCLUDED.direct_freekicks_text,
                    penalties_order = EXCLUDED.penalties_order,
                    penalties_text = EXCLUDED.penalties_text,
                    expected_goals_per_90 = EXCLUDED.expected_goals_per_90,
                    saves_per_90 = EXCLUDED.saves_per_90,
                    expected_assists_per_90 = EXCLUDED.expected_assists_per_90,
                    expected_goal_involvements_per_90 = EXCLUDED.expected_goal_involvements_per_90,
                    expected_goals_conceded_per_90 = EXCLUDED.expected_goals_conceded_per_90,
                    goals_conceded_per_90 = EXCLUDED.goals_conceded_per_90,
                    now_cost_rank = EXCLUDED.now_cost_rank,
                    now_cost_rank_type = EXCLUDED.now_cost_rank_type,
                    form_rank = EXCLUDED.form_rank,
                    form_rank_type = EXCLUDED.form_rank_type,
                    points_per_game_rank = EXCLUDED.points_per_game_rank,
                    points_per_game_rank_type = EXCLUDED.points_per_game_rank_type,
                    selected_rank = EXCLUDED.selected_rank,
                    selected_rank_type = EXCLUDED.selected_rank_type,
                    starts_per_90 = EXCLUDED.starts_per_90,
                    clean_sheets_per_90 = EXCLUDED.clean_sheets_per_90,
                    defensive_contribution_per_90 = EXCLUDED.defensive_contribution_per_90;
            """

            for player in data:
                cursor.execute(qry, (
                    player.get("id"), player.get("code"), player.get("dreamteam_count"),
                    player.get("element_type"), player.get("ep_next"), player.get("ep_this"),
                    player.get("event_points"), player.get("first_name"), player.get("form"),
                    player.get("in_dreamteam"), player.get("now_cost"), player.get("points_per_game"),
                    player.get("second_name"), player.get("selected_by_percent"), player.get("special"),
                    player.get("team"), player.get("team_code"), player.get("total_points"),
                    player.get("transfers_in"), player.get("transfers_in_event"), player.get("transfers_out"),
                    player.get("transfers_out_event"), player.get("value_form"), player.get("value_season"),
                    player.get("web_name"), player.get("region"), player.get("team_join_date"),
                    player.get("birth_date"), player.get("has_temporary_code"), player.get("minutes"),
                    player.get("goals_scored"), player.get("assists"), player.get("clean_sheets"),
                    player.get("goals_conceded"), player.get("own_goals"), player.get("penalties_saved"),
                    player.get("penalties_missed"), player.get("yellow_cards"), player.get("red_cards"),
                    player.get("saves"), player.get("bonus"), player.get("bps"), player.get("influence"),
                    player.get("creativity"), player.get("threat"), player.get("ict_index"),
                    player.get("clearances_blocks_interceptions"), player.get("recoveries"), player.get("tackles"),
                    player.get("defensive_contribution"), player.get("starts"), player.get("expected_goals"),
                    player.get("expected_assists"), player.get("expected_goal_involvements"),
                    player.get("expected_goals_conceded"), player.get("influence_rank"), player.get("influence_rank_type"),
                    player.get("creativity_rank"), player.get("creativity_rank_type"), player.get("threat_rank"),
                    player.get("threat_rank_type"), player.get("ict_index_rank"), player.get("ict_index_rank_type"),
                    player.get("corners_and_indirect_freekicks_order"), player.get("corners_and_indirect_freekicks_text"),
                    player.get("direct_freekicks_order"), player.get("direct_freekicks_text"),
                    player.get("penalties_order"), player.get("penalties_text"), player.get("expected_goals_per_90"),
                    player.get("saves_per_90"), player.get("expected_assists_per_90"), player.get("expected_goal_involvements_per_90"),
                    player.get("expected_goals_conceded_per_90"), player.get("goals_conceded_per_90"), player.get("now_cost_rank"),
                    player.get("now_cost_rank_type"), player.get("form_rank"), player.get("form_rank_type"),
                    player.get("points_per_game_rank"), player.get("points_per_game_rank_type"),
                    player.get("selected_rank"), player.get("selected_rank_type"), player.get("starts_per_90"),
                    player.get("clean_sheets_per_90"), player.get("defensive_contribution_per_90")
                ))
            conn.commit()
            logger.info("Player data saved successfully")

        except Exception as e:
            logger.error(f"Error saving Player data {e}")

    except Exception as e:
        logger.error(f"Error downloading league data {e}")

def connect_to_db():
    """ Connect to Postgres Database"""
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

def extract_player(league_id,player_id):
    """ Extract details of a single player in a league.
        Details would be used to game week table"""
    
    all_results = []
    page = 1
    has_next = True

    # Loop through all the pages for leagues with more than 50 players
    while has_next:
        league_URL = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/?page_standings={page}"
        try:
            response = requests.get(league_URL)
            response.raise_for_status()
            data = response.json()

            results = data.get("standings", {}).get("results", [])
            all_results.extend(results)

            has_next = data.get("standings", {}).get("has_next", False)
            page += 1
        except Exception as e:
            logger.error(f"Error getting player details: {e}")
            break

        if all_results is None:
            logger.warning("Not found")


    # return details of the filtered player
    filtered = [player for player in all_results if player['entry'] == player_id]
    return filtered


def extract_players_info(league_id, conn):
    """ Extracts all player information in a league"""
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
                    INSERT INTO fpl.player_details (team_id,player_name,team_name,total_point)
                                    VALUES(%s,%s,%s,%s)
                    ON CONFLICT (team_id) DO UPDATE
                    SET team_name = EXCLUDED.team_name
                    WHERE fpl.player_details.team_name IS DISTINCT FROM EXCLUDED.team_name;
                    """
            players_data = [(row["entry"],row["player_name"],row["entry_name"],row["event_total"] ) for row in all_results]
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
    """ Extract basic details of a league"""
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

def extract_gw_data(league_id, player_ids, conn):
    """ Extracts information for game week data with progress logging """
    import time

    logger.info(" ***Extracting information for game week data*** ")
    all_data = {}
    total_players = len(player_ids)

    # Step 1: Fetch all player history data
    for i, player in enumerate(player_ids, start=1):
        game_week_URL = f"https://fantasy.premierleague.com/api/entry/{player}/history"
        try:
            response = requests.get(game_week_URL)
            response.raise_for_status()
            data = response.json()['current']

            if not data:
                logger.warning(f"No data found for player {player}")
                continue

            all_data[player] = data

            # Display progress in console
            print(f"[{i}/{total_players}] Fetched GW data for player {player}")
        except Exception as e:
            logger.error(f"Error downloading game week data for player {player}: {e}")

        # Optional: small delay to avoid hitting API too hard
        time.sleep(0.1)

    # Step 2: Save data to DB
    try:
        cursor = conn.cursor()
        qry = """
            INSERT INTO fpl.gw_events (
                game_week, weeks_points, total_points, bank, transfers, transfer_cost, gross_points,
                bench_points, team_id, league_id, overall_rank, team_value, league_rank, last_league_rank, league_rank_sort
            )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (game_week,team_id,league_id) DO UPDATE SET
                weeks_points = EXCLUDED.weeks_points,
                total_points = EXCLUDED.total_points,
                bank = EXCLUDED.bank,
                transfers = EXCLUDED.transfers,
                transfer_cost = EXCLUDED.transfer_cost,
                gross_points = EXCLUDED.gross_points,
                bench_points = EXCLUDED.bench_points,
                overall_rank = EXCLUDED.overall_rank,
                team_value = EXCLUDED.team_value,
                league_rank = EXCLUDED.league_rank,
                last_league_rank = EXCLUDED.last_league_rank,
                league_rank_sort = EXCLUDED.league_rank_sort;
        """

        for j, (player_id, results) in enumerate(all_data.items(), start=1):
            for result in results:
                gross_point = result["points"] + result["event_transfers_cost"]

                # Get league ranks
                player_ranks = extract_player(league_id, player_id)
                league_rank = player_ranks[0].get('rank')
                last_league_rank = player_ranks[0].get('last_rank')
                league_sort_rank = player_ranks[0].get('rank_sort')

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
                    result['value'],
                    league_rank,
                    last_league_rank,
                    league_sort_rank
                )

                cursor.execute(qry, values)

            # Show DB insert progress
            print(f"[{j}/{total_players}] Saved GW data for player {player_id}")

        conn.commit()
        logger.info("Gameweek data saved successfully")       

    except Exception as e:
        logger.error(f"Error saving game week data {e}")

def main():
    conn = connect_to_db()
    league_id = os.getenv("LEAGUE_ID")

    element_types(conn)
    extract_teams(conn)
    extract_elements(conn)

    extract_league_data(league_id=league_id, conn=conn)
    player_ids=extract_players_info(league_id=league_id, conn=conn)
    extract_gw_data(league_id,player_ids, conn=conn)
    
if __name__ == "__main__":
    main()