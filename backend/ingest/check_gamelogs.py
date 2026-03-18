import os, time
from dotenv import load_dotenv
load_dotenv()

try:
    from nba_api.stats.endpoints import PlayerGameLogs
    time.sleep(2)
    ep = PlayerGameLogs(
        season_nullable='2025-26',
        season_type_nullable='Regular Season',
        league_id_nullable='00',
        measure_type_player_game_logs_nullable='Base'
    )
    df = ep.get_data_frames()[0]
    print("Columns:", list(df.columns[:15]))
    print("Shape:", df.shape)
    print("Sample GP values:", df['GP'].unique()[:10] if 'GP' in df.columns else "NO GP COLUMN")
    # Check one player
    sample = df[df['PLAYER_NAME'] == 'Stephen Curry']
    print(f"\nCurry rows: {len(sample)}")
    if not sample.empty:
        print(sample[['PLAYER_NAME', 'GAME_DATE', 'GP', 'MIN', 'PTS']].head(5).to_string())
except Exception as e:
    print("Error:", e)