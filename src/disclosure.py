""" This module does the disclosure of the BMI survey. """
import traceback
import Algorithmia
import pandas as pd

client = Algorithmia.client()


def _get_fh(data_url):
    """
    Opens Algorithmia data urls and returns a file object.

    :param data_url: data url to open.
    :return: file object.
    :raises: AlgorithmException.
    """
    try:
        fh = client.file(data_url).getFile()
    except Algorithmia.errors.DataApiError as exc:
        raise Algorithmia.errors.AlgorithmException(
            "Unable to get datafile {}: {}".format(
                data_url, ''.join(exc.args)
            )
        )
    return fh


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.

    :param exception: Exception object.
    :return: String.
    """
    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def apply(input):
    """
    Main method that runs Disclosure function(s).
    :param input: JSON string.
    :return: JSON string.
    """
    cur_period = 201706
    try:
        disclos_df = disclosure(
            _get_fh("s3://".format(input["s3Pointer"])), cur_period
        )
    except Algorithmia.errors.AlgorithmException as exc:
        return {
            "success": False,
            "error": _get_traceback(exc)
        }
    except Exception as exc:
        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    client.file("s3://es-algo-poc/testing.json").putJson(
        disclos_df.apply(
            lambda x: x.to_json(
                orient='records'
            )
            [1:-1].replace('}, {', '},{'))
    )

    return {
        "success": True,
        "data": disclos_df.apply(
            lambda x: x.to_json(
                orient="records"
            )
            [1:-1].replace("}, {", "},{")
        )
    }


def disclosure(input_df, cur_period):
    """
    Reading in a csv, converting to a data frame and converting some cols to int.
    :param input_df: The csv file that is converted into a data frame.
    :param cur_period: The current period for the results process.
    :return: None.
    """

    input_df = pd.read_csv(input_df, dtype={"Q601_asphalting_sand": int,
                                            'Q602_building_soft_sand': int,
                                            'Q603_concreting_sand': int,
                                            'Q604_bituminous_gravel': int,
                                            'Q605_concreting_gravel': int,
                                            'Q606_other_gravel': int,
                                            'Q607_constructional_fill': int,
                                            'Q608_total': int,
                                            'enterprise_ref': int, 'period': int,
                                            'region': int})

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
