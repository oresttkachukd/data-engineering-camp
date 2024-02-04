import inflection

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print('Before transofrmation applied:', len(data))
    data = data[data['passenger_count'] > 0]
    data = data[data['trip_distance'] > 0]
    print('After transofrmation applied:', len(data))
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    print('Columns to rename: ', [col for col in data.columns if inflection.underscore(col) != col])
    data.columns = [inflection.underscore(col) for col in data.columns]

    print('Vendors: ', ', '.join(map(str, data['vendor_id'].unique())))

    return data


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    assert output['passenger_count'].isin([0, None]).sum() == 0, 'The output has bad records'
    assert (output['passenger_count'] > 0).all(), "Values in passenger_count should be greater than 0."
    assert output['trip_distance'].isin([0, None]).sum() == 0, 'The output has bad records'
    assert (output['trip_distance'] > 0).all(), "Values in trip_distance should be greater than 0."
    assert output['vendor_id'].isin([1, 2]).all(), "Values in vendor_id should be one of the existing values."
    assert sum(any(c.isupper() for c in col) for col in output.columns) == 0, 'The output has misnamed columns'
