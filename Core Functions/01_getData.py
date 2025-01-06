import pandas as pd
import polars as pl
import os
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Read the data
game_data_path = "C:/Users/maild/nfl-big-data-bowl-2025/games.csv"
play_data_path = "C:/Users/maild/nfl-big-data-bowl-2025/plays.csv"
player_data_path = "C:/Users/maild/nfl-big-data-bowl-2025/players.csv"
player_play_data_path = "C:/Users/maild/nfl-big-data-bowl-2025/player_play.csv"
tracking_data_path = "C:/Users/maild/nfl-big-data-bowl-2025/tracking_week*.csv"
# Load datasets into polars DataFrames
games_df = pl.read_csv(game_data_path,infer_schema_length=10000)
plays_df = pl.read_csv(play_data_path,infer_schema_length=10000)
players_df = pl.read_csv(player_data_path,infer_schema_length=10000)
player_play_df = pl.read_csv(player_play_data_path,infer_schema_length=10000)
tracking_df = pl.read_csv(tracking_data_path,infer_schema_length=10000)

# Clean tracking data of unnecessary frame data (before line is set and after ball is snapped)
tracking_df = tracking_df.filter(pl.col('frameType') != "AFTER_SNAP")
tracking_df = tracking_df.with_columns(
    (pl.col('gameId').cast(str) + '-' + pl.col('playId').cast(str)).alias('uniquePlayId'),
    (pl.col('gameId').cast(str) + '-' + pl.col('playId').cast(str) + '-' + pl.col('nflId').cast(str)).alias('uniquePlayerId'),
)
tracking_df = tracking_df.with_columns([
    pl.when(pl.col('event')=='line_set').then(pl.col('frameId')).otherwise(-1).alias('startId'),
    pl.when(pl.col('event')=='ball_snap').then(pl.col('frameId')).otherwise(-1).alias('snapId'),
])
tracking_df = tracking_df.with_columns([
    pl.col('startId').max().over(pl.col('uniquePlayId')),
    pl.col('snapId').max().over(pl.col('uniquePlayId')),
])
tracking_df = tracking_df.with_columns([
    (pl.col('frameId') - pl.col('startId')).alias('framesSinceStart'),
])
tracking_df = tracking_df.filter(pl.col('framesSinceStart') > -1)
# Set new X and Y values for later functions
tracking_df = tracking_df.with_columns([
    pl.when(pl.col('playDirection')=='right').then(53.3-pl.col('y')).otherwise(pl.col('y')).alias('adjustedX'),
    pl.when(pl.col('playDirection')=='right').then(pl.col('x')).otherwise(120-pl.col('x')).alias('adjustedY'),
])
tracking_df = tracking_df.with_columns([
    pl.when(pl.col('event')=='line_set').then(pl.col('adjustedX')).otherwise(-1).alias('startingX'),
    pl.when(pl.col('event')=='line_set').then(pl.col('adjustedY')).otherwise(-1).alias('startingY')
])
tracking_df = tracking_df.with_columns([
    pl.when(pl.col('event')=='ball_snap').then(pl.col('adjustedX')).otherwise(-1).alias('snapX'),
    pl.when(pl.col('event')=='ball_snap').then(pl.col('adjustedY')).otherwise(-1).alias('snapY')
])

# Correct orientation "o" to the adjusted  direction
tracking_df = tracking_df.with_columns([
    pl.when(pl.col('o')=='NA').then(0).otherwise(pl.col('o')).alias('CleanedO'),
])
tracking_df = tracking_df.with_columns([
    pl.when(pl.col('playDirection')=='right').then(pl.col('CleanedO').cast(pl.Float64)).otherwise((180+pl.col('CleanedO').cast(pl.Float64))%360).alias('firstAdjustedO'),
])

tracking_df = tracking_df.with_columns([
    pl.when(pl.col('firstAdjustedO') <= 180).then(180-pl.col('firstAdjustedO')).otherwise(540-pl.col('firstAdjustedO')).alias('adjustedO')
])
tracking_df = tracking_df.with_columns([
    pl.col('startingX').max().over(pl.col('uniquePlayerId')),
    pl.col('startingY').max().over(pl.col('uniquePlayerId'))
])
tracking_df = tracking_df.with_columns([
    pl.col('snapX').max().over(pl.col('uniquePlayerId')),
    pl.col('snapY').max().over(pl.col('uniquePlayerId'))
])
tracking_df = tracking_df.with_columns([
    (pl.col('snapX') - pl.col('startingX')).abs().alias('changeX'),
    (pl.col('snapY') - pl.col('startingY')).abs().alias('changeY'),
])
tracking_df = tracking_df.with_columns([
    pl.col('changeX').max().over(pl.col('uniquePlayerId')),
    pl.col('changeY').max().over(pl.col('uniquePlayerId'))
])

# Remove plays that do not have Set or Snap
tracking_df = tracking_df.filter(pl.col('startId') != -1)
tracking_df = tracking_df.filter(pl.col('snapId') != -1)

#Connect player_play and players to tracking data
player_play_df = player_play_df.with_columns([pl.col('nflId').cast(str)])
player_play_df = tracking_df.filter(pl.col('framesSinceStart') == 1).join(player_play_df,on=['gameId', 'playId','nflId'],how='left')
players_df = players_df.with_columns([pl.col('nflId').cast(str)])
players_df = player_play_df.join(players_df,on='nflId',how='left')
players_df = players_df.with_columns([pl.col('position').fill_null('football')])

# Write New CSV to "Python Created CSVs
players_df.write_csv("cleaned_data.csv")
print(players_df.head())
print(players_df.shape)