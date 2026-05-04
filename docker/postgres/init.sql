CREATE TABLE IF NOT EXISTS offlane_performance (
    match_id BIGINT,
    player_id BIGINT,
    hero_id INT,
    win BOOLEAN,
    duration INT,
    gpm INT,
    xpm INT,
    kills INT,
    deaths INT,
    assists INT,
    hero_damage BIGINT,
    tower_damage BIGINT,
    lane_efficiency_pct FLOAT,
    teamfight_participation FLOAT,
    total_damage_taken BIGINT,
    gold_at_10 INT,
    PRIMARY KEY (match_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_player_id ON offlane_performance(player_id);
CREATE INDEX IF NOT EXISTS idx_match_id ON offlane_performance(match_id);

-- View for easy player comparison
CREATE OR REPLACE VIEW player_comparison AS
SELECT 
    player_id,
    COUNT(*) as total_matches,
    ROUND((AVG(win::int) * 100)::numeric, 2) as win_rate,
    ROUND(AVG(gpm)) as avg_gpm,
    ROUND(AVG(xpm)) as avg_xpm,
    ROUND(AVG(kills)::numeric, 2) as avg_kills,
    ROUND(AVG(deaths)::numeric, 2) as avg_deaths,
    ROUND(AVG(assists)::numeric, 2) as avg_assists,
    ROUND(AVG(teamfight_participation)::numeric, 2) as avg_teamfight_participation,
    ROUND(AVG(lane_efficiency_pct)::numeric, 2) as avg_lane_efficiency,
    ROUND(AVG(total_damage_taken)) as avg_damage_taken,
    ROUND(AVG(gold_at_10)) as avg_gold_at_10
FROM offlane_performance
GROUP BY player_id;
