import pytest

from data_inclusion.pipeline.dags.publish_opendata import constants, datagouv


@pytest.mark.integration
def test_datagouv_client():
    client = datagouv.DataGouvClient(api_key=None)

    # validates :
    # - that the client is properly configured
    # - that the dataset exists
    client.get_dataset(dataset_id=constants.DATAGOUV_DI_DATASET_ID)
