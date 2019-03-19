import Algorithmia
import pandas as pd
import numpy as np
from io import StringIO
import json

def apply(input):
    cur_period = 201706
    bob = disclosure_stage_1(cur_period)
    return bob

#Disclosure stage 1 method
def disclosure_stage_1(cur_period):
    #client = Algorithmia.client()
    #file = client.file("")


    #file = ""
    df = pd.read_csv(file, dtype={"Q601_asphalting_sand": int, 'Q602_building_soft_sand': int,
                                             'Q603_concreting_sand': int, 'Q604_bituminous_gravel': int,
                                             'Q605_concreting_gravel': int, 'Q606_other_gravel': int,
                                             'Q607_constructional_fill': int, 'Q608_total': int,
                                             'enterprise_ref': int, 'period': int, 'region': int})

    df["disclosive"] = None
    df["publish"] = None
    df["reason"] = None

    def runDisclosure(row):

        if(row['Q608_total'] == 0):
            row['disclosive'] = 'N'
            row['publish'] = 'Publish'
            row['reason'] = ' Total is zero'
        else:
            row['disclosive'] = 'Y'
            row['publish'] = 'N/A'
        return row

    disaggregatedData = df[df.period == cur_period]

    regionagg = disaggregatedData.groupby('region')


    regionagg = regionagg.agg({'Q608_total': 'sum', 'Q607_constructional_fill': 'sum', 'Q606_other_gravel': 'sum', 'Q605_concreting_gravel': 'sum', 'Q604_bituminous_gravel': 'sum', 'Q603_concreting_sand': 'sum', 'Q602_building_soft_sand': 'sum', 'Q601_asphalting_sand': 'sum', 'enterprise_ref': 'nunique'})
    regionagg = regionagg.apply(runDisclosure, axis=1)
    print("end result\n", regionagg)
    regionlorm = disaggregatedData.groupby(['region'])
    regionagglorm = disaggregatedData.groupby(['region', 'land_or_marine'])
    print("this is regionlorm\n", regionagg)
    print("this is regionagglorm\n", regionagglorm)