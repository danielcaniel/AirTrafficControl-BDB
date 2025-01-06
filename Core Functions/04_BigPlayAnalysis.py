import pandas as pd
import polars as pl
import altair
import numpy as np
import plotly.express as px
import seaborn as sns
from matplotlib import pyplot as plt
import os
import warnings

#Load Data
motionPlays_file_path = "C:/Users/maild/PycharmProjects/AirTrafficControl/Python Created CSVs/PlayersWithMotionLabeled.csv"

motionPlays_df = pl.read_csv(motionPlays_file_path, infer_schema_length=100000)

#Filter to only the metrics we want to measure

motionPlays_df = motionPlays_df.filter(pl.col("motionThisPlay")==1).select([
    "motionType", "expectedPointsAdded", "yardsGained"
])
motionPlays_df = motionPlays_df.with_columns([
    pl.when(pl.col("yardsGained") >= 15).then(pl.lit("YES")).otherwise(pl.lit("NO")).alias("bigPlay"),
    pl.when(pl.col("expectedPointsAdded") > 0).then(pl.lit("YES")).otherwise(pl.lit("NO")).alias("successfulPlay")
])

#Honestly I don't know how to make tables and graphs in python yet, I exported this to Excel and made a table there
motionPlays_df.write_csv("finalMotions.csv")