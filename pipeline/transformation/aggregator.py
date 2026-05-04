import logging

logger = logging.getLogger(__name__)

class MetricsAggregator:
    """
    Transforms raw match data into aggregated offlane metrics.
    """
    @staticmethod
    def is_offlane(player: dict) -> bool:
        """
        Robust heuristic for identifying the offlaner (Pos 3).
        In OpenDota:
        - lane_role 3 is the primary indicator.
        - lane 1 for Dire or 3 for Radiant can be fallbacks.
        """
        lane_role = player.get("lane_role")
        if lane_role == 3:
            return True
        
        # Fallback heuristic if lane_role is missing or unreliable
        lane = player.get("lane")
        is_radiant = player.get("isRadiant")
        
        # Radiant Offlane is Lane 3 (Top)
        # Dire Offlane is Lane 1 (Bottom)
        if is_radiant and lane == 3:
            return True
        if not is_radiant and lane == 1:
            return True
            
        return False

    @staticmethod
    def extract_offlane_stats(match_data: dict) -> List[dict]:
        """
        Extracts metrics for all offlane players in a match.
        """
        offlane_stats = []
        players = match_data.get("players", [])
        
        # Calculate total kills per team for teamfight participation
        radiant_kills = sum(p.get("kills", 0) for p in players if p.get("isRadiant"))
        dire_kills = sum(p.get("kills", 0) for p in players if not p.get("isRadiant"))

        for p in players:
            if MetricsAggregator.is_offlane(p):
                player_stats = {
                    "match_id": match_data.get("match_id"),
                    "player_id": p.get("account_id"),
                    "hero_id": p.get("hero_id"),
                    "win": bool(p.get("win")),
                    "duration": match_data.get("duration"),
                    "gpm": p.get("gold_per_min"),
                    "xpm": p.get("xp_per_min"),
                    "kills": p.get("kills"),
                    "deaths": p.get("deaths"),
                    "assists": p.get("assists"),
                    "hero_damage": p.get("hero_damage"),
                    "tower_damage": p.get("tower_damage"),
                    "lane_efficiency_pct": p.get("lane_efficiency_pct"),
                    "total_damage_taken": sum(p.get("damage_taken", {}).values()) if isinstance(p.get("damage_taken"), dict) else 0,
                }

                # Teamfight participation
                team_kills = radiant_kills if p.get("isRadiant") else dire_kills
                participation = 0
                if team_kills > 0:
                    participation = (p.get("kills", 0) + p.get("assists", 0)) / team_kills
                player_stats["teamfight_participation"] = participation

                # Gold at 10 minutes
                gold_t = p.get("gold_t")
                if isinstance(gold_t, list) and gold_t:
                    player_stats["gold_at_10"] = gold_t[10] if len(gold_t) > 10 else gold_t[-1]
                else:
                    # Fallback to total_gold if gold_t is missing (though less accurate for "at 10m")
                    player_stats["gold_at_10"] = p.get("total_gold", 0)

                offlane_stats.append(player_stats)
        
        return offlane_stats
