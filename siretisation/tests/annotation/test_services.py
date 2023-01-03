import pytest

from annotation import models, services

pytestmark = pytest.mark.django_db


def test_import_dataset(group):
    services.import_dataset(
        dataset_rows_list=[{"foo": 1}, {"bar": 2}],
        dataset_label="foo",
        organization_id=group.id,
    )

    assert models.DatasetRow.objects.count() == 2


def test_import_twice(group):
    for _ in range(2):
        services.import_dataset(
            dataset_rows_list=[{"foo": 1}, {"bar": 2}],
            dataset_label="foo",
            organization_id=group.id,
        )

    assert models.DatasetRow.objects.count() == 4
