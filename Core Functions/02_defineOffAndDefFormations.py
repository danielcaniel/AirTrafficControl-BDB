import pandas as pd
import polars as pl
import numpy as np
import os
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Filepaths for Data
play_data_path = "C:/Users/maild/nfl-big-data-bowl-2025/plays.csv"
clean_data_path = "C:/Users/maild/PycharmProjects/AirTrafficControl/Python Created CSVs/cleaned_data.csv"

#Create polars Dataframe
plays_df = pl.read_csv(play_data_path,infer_schema_length=10000)
clean_df = pl.read_csv(clean_data_path, infer_schema_length=10000)
clean_df = clean_df.join(plays_df,on=['gameId','playId'],how='left')

#Create Position Groups
offSkillPositions = ["RB","WR"]
offHammerPositions = ["TE","FB","QB"]
offLinePositions = ["T","C","G"]
defSkillPositions = ["CB","FS", "SS", "DB"]
defHammerPositions = ["MLB","ILB","OLB", "LB"]
defLinePositions = ["DE","NT","DT"]
offPrimaryBlockers = ["T","C","G","TE","FB"]
offPassCatchers = ["RB", "WR", "TE"]

#Assign players as Offense or Defense

clean_df = clean_df.with_columns([
    pl.when(pl.col('club')==pl.col('defensiveTeam')).then(pl.lit("DEF"))
    .when(pl.col('club')==pl.col('possessionTeam')).then(pl.lit("OFF"))
    .otherwise(pl.lit("BALL")).alias("PlayerUnit")
])

#Check who is in the core of the offensive formation / relabel who is snapping the ball
clean_df = clean_df.with_columns([
    pl.when(pl.col("PlayerUnit")=="OFF").then(pl.col("startingX").over("uniquePlayId").alias("OFFstartX"))
])
clean_df = clean_df.with_columns([
    pl.when(pl.col('displayName') == 'football').then(pl.col('startingX')).otherwise(0).max().over('uniquePlayId')
    .alias('ballX'),

    pl.when(pl.col('displayName') == 'football').then(pl.col('startingY')).otherwise(0).max().over('uniquePlayId')
    .alias('ballY'),
])

clean_df = clean_df.filter(pl.col('position') != 'football')
clean_df = clean_df.with_columns([
    (pl.col('startingX') - pl.col('ballX')).alias('xFromBall'),
    (pl.col('startingY') - pl.col('ballY')).alias('yFromBall'),
])

clean_df = clean_df.with_columns([
    (pl.col('xFromBall').pow(2) + pl.col('yFromBall').pow(2)).sqrt().alias('disFromBall')
])
clean_df = clean_df.with_columns([
    pl.when(pl.col("PlayerUnit")=="OFF").then(pl.col("OFFstartX").rank("ordinal").over("uniquePlayId").alias("offRank"))
])
clean_df = clean_df.with_columns([
    pl.when(
        (pl.col('disFromBall') <= 1) &
        (pl.col('position').is_in(offLinePositions))
    )
    .then(pl.lit('Center').alias("newPosition"))
])
clean_df = clean_df.with_columns([
    pl.when(
        (pl.col('position').is_in(offLinePositions))
    )
    .then(pl.lit('inCore').alias("oLineCore"))
])

#Create Tackle Box
clean_df = clean_df.with_columns([
    pl.when((pl.col('oLineCore')=="inCore")).then(pl.col("startingX")).alias("TackleBoxX"),
    pl.when((pl.col('oLineCore')=="inCore")).then(pl.col("startingY")).alias("TackleBoxY")
])
clean_df = clean_df.with_columns([
    pl.col('TackleBoxX').max().over(pl.col('uniquePlayId')).alias("TackleBoxLeftX"),
    pl.col('TackleBoxX').min().over(pl.col('uniquePlayId')).alias("TackleBoxRightX"),
    pl.col('TackleBoxY').max().over(pl.col('uniquePlayId')).alias("TackleBoxDefY")
])
clean_df = clean_df.with_columns([
    (pl.col('TackleBoxLeftX') + 5).over(pl.col('uniquePlayId')),
    (pl.col('TackleBoxRightX') - 5).over(pl.col('uniquePlayId')),
    (pl.col('TackleBoxDefY') + 6).over(pl.col('uniquePlayId'))
])
#Evaluate how many defenders are in the tackle box
clean_df = clean_df.with_columns([
    pl.when(
        (pl.col('PlayerUnit')=="DEF") &
        (pl.col('snapX') <= pl.col('TackleBoxLeftX')) &
        (pl.col('snapX') >= pl.col('TackleBoxRightX')) &
        (pl.col('snapY') <= pl.col('TackleBoxDefY'))
    ).then(1).otherwise(0).sum().over('uniquePlayId').alias("DefInBox")
])

# clean_df.write_csv("wTackleBox.csv")

