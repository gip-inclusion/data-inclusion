import os

import pytest
import requests

from data_inclusion.pipeline.dags.import_mes_aides import constants, utils


@pytest.mark.integration
def test_mes_aides_client():
    client = utils.MesAidesClient(
        base_url=constants.API_URL,
        token=os.environ["MES_AIDES_API_KEY"],
    )

    try:
        client.list_aides()
        client.list_garages()
    except requests.HTTPError as err:
        pytest.fail(str(err))
