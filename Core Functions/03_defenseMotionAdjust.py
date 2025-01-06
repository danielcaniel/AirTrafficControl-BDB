import pandas as pd
import polars as pl
import numpy as np
import os
import warnings

tBox_data_path = "C:/Users/maild/PycharmProjects/AirTrafficControl/Python Created CSVs/wTackleBox.csv"

tBox_df = pl.read_csv(tBox_data_path, infer_schema_length=100000)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

#Evaluate Defensive response to motion
tBox_df = tBox_df.with_columns([
    pl.when(pl.col('motionSinceLineset')=='TRUE').then(1).otherwise(0).alias("motionThisPlay"),
    pl.when(pl.col('shiftSinceLineset')=='TRUE').then(1).otherwise(0).alias("shiftThisPlay")
])
tBox_df = tBox_df.with_columns([
    pl.col("motionThisPlay").max().over(pl.col('uniquePlayId')),
    pl.col("shiftThisPlay").max().over(pl.col('uniquePlayId'))
])

# Remove plays that have a shift or do not have motion
tBox_df = tBox_df.filter(pl.col('motionThisPlay') == 1)
tBox_df = tBox_df.filter(pl.col('shiftThisPlay') == 0)

motionMen_df = tBox_df.filter(pl.col('motionSinceLineset')=='TRUE')
motionMen_df = motionMen_df.with_columns([
    pl.when((pl.col('motionSinceLineset')=='TRUE'))
    .then(pl.col('changeX').pow(2) + pl.col('changeY').pow(2)).sqrt().alias('distChange')
])
# Identify Offensive Motions (This is incredibly jank and I'm sure this could be done cleaner but I'm literally out of time so BEAR WITH ME)
motionMen_df = motionMen_df.with_columns([
    #TIC
    pl.when(
        (pl.col('startingX') > pl.col('ballX')) &
        (pl.col('ballX') > pl.col('snapX')) &
        (pl.col('inMotionAtBallSnap') == 'FALSE') &
        (pl.col('distChange') < 1)
    )
    .then(3)
    #TIC
    .when(
        (pl.col('startingX') < pl.col('ballX')) &
        (pl.col('ballX') < pl.col('snapX')) &
        (pl.col('inMotionAtBallSnap') == 'FALSE') &
        (pl.col('distChange') < 1)
    )
    .then(3)
    #QUICK
    .when(
        (pl.col('inMotionAtBallSnap') == 'TRUE') &
        (pl.col('distChange') <= 3)
    )
    .then(1)
    #FLY
    .when(
        (pl.col('inMotionAtBallSnap') == 'TRUE') &
        (pl.col('distChange') > 3)
    )
    .then(2)
    #UP
    .when(
        (pl.col('startingX') > pl.col('TackleBoxRightX')) &
        (pl.col('TackleBoxRightX') < pl.col('snapX')) &
        (pl.col('startingX') < pl.col('TackleBoxLeftX')) &
        (pl.col('TackleBoxLeftX') > pl.col('snapX')) &
        (pl.col('inMotionAtBallSnap') == 'FALSE') &
        (pl.col('changeY') > 2)
    )
    .then(4)
    #TRADE
    .when(
        (pl.col('startingX') < pl.col('ballX')) &
        (pl.col('ballX') < pl.col('snapX')) &
        (pl.col('startingX') > pl.col('TackleBoxRightX')) &
        (pl.col('TackleBoxRightX') < pl.col('snapX')) &
        (pl.col('startingX') < pl.col('TackleBoxLeftX')) &
        (pl.col('TackleBoxLeftX') > pl.col('snapX')) &
        (pl.col('inMotionAtBallSnap') == 'FALSE') &
        (pl.col('distChange') >= 1)
    )
    .then(5)
    .when(
        (pl.col('startingX') > pl.col('ballX')) &
        (pl.col('ballX') > pl.col('snapX')) &
        (pl.col('startingX') > pl.col('TackleBoxRightX')) &
        (pl.col('TackleBoxRightX') < pl.col('snapX')) &
        (pl.col('startingX') < pl.col('TackleBoxLeftX')) &
        (pl.col('TackleBoxLeftX') > pl.col('snapX')) &
        (pl.col('inMotionAtBallSnap') == 'FALSE') &
        (pl.col('distChange') >= 1)
    )
    .then(5)
    #OUT
    .when(
        (pl.col('startingX') > pl.col('TackleBoxRightX')) &
        (pl.col('startingX') < pl.col('TackleBoxLeftX')) &
        (pl.col('TackleBoxRightX') > pl.col('snapX'))
    )
    .then(6)
    .when(
        (pl.col('startingX') > pl.col('TackleBoxRightX')) &
        (pl.col('startingX') < pl.col('TackleBoxLeftX')) &
        (pl.col('TackleBoxLeftX') < pl.col('snapX'))
    )
    .then(6)
    #RETURN
    .when(
        (pl.col('startingX') < pl.col('ballX')) &
        (pl.col('ballX') > pl.col('snapX')) &
        (pl.col('distChange') < 1)
    )
    .then(7)
    .when(
        (pl.col('startingX') > pl.col('ballX')) &
        (pl.col('ballX') < pl.col('snapX')) &
        (pl.col('distChange') < 1)
    )
    .then(7)
    #IN
    .when(
        (pl.col('startingX') > pl.col('TackleBoxLeftX')) &
        (pl.col('TackleBoxLeftX') < pl.col('snapX')) &
        (pl.col("changeX") > 1.25) &
        (pl.col('startingX')-pl.col('snapX') > 0)
    )
    .then(8)
    .when(
        (pl.col('startingX') < pl.col('TackleBoxRightX')) &
        (pl.col('TackleBoxRightX') > pl.col('snapX')) &
        (pl.col("changeX") > 1.25) &
        (pl.col('startingX')-pl.col('snapX') < 0)
    )
    .then(8)
    .when(
        (pl.col('startingX') < pl.col('ballX')) &
        (pl.col('ballX') > pl.col('snapX')) &
        (pl.col('startingX') - pl.col('snapX') < 0)
    )
    .then(8)
    .when(
        (pl.col('startingX') > pl.col('ballX')) &
        (pl.col('ballX') < pl.col('snapX')) &
        (pl.col('startingX') - pl.col('snapX') > 0)
    )
    .then(8)
    #OUT
    .when(
        (pl.col('startingX') > pl.col('TackleBoxLeftX')) &
        (pl.col('TackleBoxLeftX') < pl.col('snapX')) &
        (pl.col("changeX") > 1.25) &
        (pl.col('startingX')-pl.col('snapX') <= 0)
    )
    .then(9)
    .when(
        (pl.col('startingX') < pl.col('TackleBoxRightX')) &
        (pl.col('TackleBoxRightX') > pl.col('snapX')) &
        (pl.col("changeX") > 1.25) &
        (pl.col('startingX')-pl.col('snapX') >= 0)
    )
    .then(9)
    .when(
        (pl.col('startingX') < pl.col('TackleBoxLeftX')) &
        (pl.col('TackleBoxLeftX') < pl.col('snapX')) &
        (pl.col("changeX") > 1.25)
    )
    .then(9)
    .when(
        (pl.col('startingX') > pl.col('TackleBoxRightX')) &
        (pl.col('TackleBoxRightX') > pl.col('snapX')) &
        (pl.col("changeX") > 1.25)
    )
    .then(9)
    #ACROSS
    .when(
        (pl.col('startingX') > pl.col('ballX')) &
        (pl.col('ballX') > pl.col('snapX')) &
        (pl.col('TackleBoxRightX') > pl.col('snapX')) &
        (pl.col('startingX') > pl.col('TackleBoxLeftX')) &
        (pl.col('distChange') >= 1)
    )
    .then(10)
    .when(
        (pl.col('startingX') < pl.col('ballX')) &
        (pl.col('ballX') < pl.col('snapX')) &
        (pl.col('startingX') < pl.col('TackleBoxRightX')) &
        (pl.col('TackleBoxLeftX') < pl.col('snapX')) &
        (pl.col('distChange') >= 1)
    )
    .then(10)
    .otherwise(pl.lit("Unknown")).alias("motionTypeEncoded")
])
# Again, incredibly jank but idk how dictionaries work yet
motionMen_df = motionMen_df.filter(pl.col("motionTypeEncoded") != "Unknown")
motionMen_df = motionMen_df.with_columns([
    pl.when(pl.col("motionTypeEncoded") == "1").then(pl.lit("QUICK"))
    .when(pl.col("motionTypeEncoded") == "2").then(pl.lit("FLY"))
    .when(pl.col("motionTypeEncoded") == "3").then(pl.lit("TIC"))
    .when(pl.col("motionTypeEncoded") == "4").then(pl.lit("UP"))
    .when(pl.col("motionTypeEncoded") == "5").then(pl.lit("ACROSS"))
    .when(pl.col("motionTypeEncoded") == "6").then(pl.lit("OUT"))
    .when(pl.col("motionTypeEncoded") == "7").then(pl.lit("RETURN"))
    .when(pl.col("motionTypeEncoded") == "8").then(pl.lit("IN"))
    .when(pl.col("motionTypeEncoded") == "9").then(pl.lit("OUT"))
    .when(pl.col("motionTypeEncoded") == "10").then(pl.lit("ACROSS"))
    .alias("motionType")
])
# motionMen_df.write_csv("PlayersWithMotionLabeled.csv")
