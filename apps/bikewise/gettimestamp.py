from datetime import date
from datetime import datetime
import pandas as pd

def get_timestamp(filedate):
    occuredafter = int((filedate - pd.DateOffset(hours=0)).timestamp()) 
    occuredbefore = int((filedate + pd.DateOffset(hours=24)).timestamp())
    return [occuredafter,occuredbefore]
