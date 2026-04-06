# credit_spread/spread_builder.py
import pandas as pd
import numpy as np
from itertools import combinations
from scipy.stats import norm
import time
import yfinance as yf
import os

def get_spread_recommendations():
    if os.path.exists(r"C:\Users\Lenovo\PycharmProjects\Trading_System\dashboard\data"):
        print("Folder exists")
    else:
        print("Folder does not exist")





if __name__ == "__main__":
    get_spread_recommendations()


