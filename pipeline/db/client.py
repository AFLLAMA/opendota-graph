import psycopg2
from psycopg2.extras import execute_values
import os

class PostgresClient:
    def __init__(self, dsn: str = None):
        if not dsn:
            # Default to env vars or common docker settings
            user = os.getenv("POSTGRES_USER", "postgres")
            password = os.getenv("POSTGRES_PASSWORD", "postgres")
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            dbname = os.getenv("POSTGRES_DB", "opendota")
            dsn = f"dbname={dbname} user={user} password={password} host={host} port={port}"
        
        self.dsn = dsn
        self.conn = None

    def connect(self):
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(self.dsn)
            self.conn.autocommit = True

    def insert_stats(self, stats_list: list):
        """
        Batch inserts stats into the database.
        """
        if not stats_list:
            return

        self.connect()
        query = """
            INSERT INTO offlane_performance (
                match_id, player_id, hero_id, win, duration, gpm, xpm, 
                kills, deaths, assists, hero_damage, tower_damage, 
                lane_efficiency_pct, teamfight_participation, 
                total_damage_taken, gold_at_10
            ) VALUES %s
            ON CONFLICT (match_id, player_id) DO UPDATE SET
                win = EXCLUDED.win,
                duration = EXCLUDED.duration,
                gpm = EXCLUDED.gpm,
                xpm = EXCLUDED.xpm,
                kills = EXCLUDED.kills,
                deaths = EXCLUDED.deaths,
                assists = EXCLUDED.assists,
                hero_damage = EXCLUDED.hero_damage,
                tower_damage = EXCLUDED.tower_damage,
                lane_efficiency_pct = EXCLUDED.lane_efficiency_pct,
                teamfight_participation = EXCLUDED.teamfight_participation,
                total_damage_taken = EXCLUDED.total_damage_taken,
                gold_at_10 = EXCLUDED.gold_at_10;
        """
        
        # Prepare data for execute_values
        data = [
            (
                s["match_id"], s["player_id"], s["hero_id"], s["win"], s["duration"], 
                s["gpm"], s["xpm"], s["kills"], s["deaths"], s["assists"], 
                s["hero_damage"], s["tower_damage"], s["lane_efficiency_pct"], 
                s["teamfight_participation"], s["total_damage_taken"], s["gold_at_10"]
            )
            for s in stats_list if s["player_id"] is not None # Skip anonymous players
        ]

        with self.conn.cursor() as cur:
            execute_values(cur, query, data)

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
