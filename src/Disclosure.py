import json
from io import StringIO
import Algorithmia
import pandas as pd


def apply(input):
    cur_period = 201706
    bob = disclosure_stage_1(cur_period)
    return bob


# Disclosure stage 1 method
def disclosure_stage_1(cur_period):
    client = Algorithmia.client()
    file = client.file("")
    # df = pd.read_csv(file)

    # file = ""
    input_df = pd.read_csv(file, dtype={"Q601_asphalting_sand": int, 'Q602_building_soft_sand': int,
                                  'Q603_concreting_sand': int, 'Q604_bituminous_gravel': int,
                                  'Q605_concreting_gravel': int, 'Q606_other_gravel': int,
                                  'Q607_constructional_fill': int, 'Q608_total': int,
                                  'enterprise_ref': int, 'period': int, 'region': int})

    input_df["disclosive"] = None
    input_df["publish"] = None
    input_df["reason"] = None

    def run_disclosure(row):

        if row['Q608_total'] == 0:
            row['disclosive'] = 'N'
            row['publish'] = 'Publish'
            row['reason'] = ' Total is zero'
        else:
            row['disclosive'] = 'Y'
            row['publish'] = 'N/A'
        return row

    disaggregated_data = input_df[input_df.period == cur_period]

    region_agg = disaggregated_data.groupby('region')

    region_agg = region_agg.agg({'Q608_total': 'sum', 'Q607_constructional_fill': 'sum',
                                 'Q606_other_gravel': 'sum', 'Q605_concreting_gravel': 'sum',
                                 'Q604_bituminous_gravel': 'sum', 'Q603_concreting_sand': 'sum',
                                 'Q602_building_soft_sand': 'sum', 'Q601_asphalting_sand': 'sum',
                                 'enterprise_ref': 'nunique'})
    region_agg = region_agg.apply(run_disclosure, axis=1)
    print("end result\n", region_agg)
    # regionlorm = disaggregated_data.groupby(['region'])
    region_agg_lorm = disaggregated_data.groupby(['region', 'land_or_marine'])
    print("this is regionlorm\n", region_agg)
    print("this is regionagglorm\n", region_agg_lorm)

    return region_agg_lorm

disclosure_stage_1(201706)
