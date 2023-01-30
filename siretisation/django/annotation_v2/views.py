from django.contrib.postgres import search
from django import http
from django.shortcuts import render

from annotation_v2.models import Datalake


def index(request: http.HttpRequest):
    context = {
        "left_dataset_label_str": "1jeune1solution",
        "right_dataset_label_str": "mes_aides_aides",
        "left_row_instance": Datalake.objects.filter(src_alias="1jeune1solution", file__contains="benefits").first(),
        "column_names": ["id", "fields.Nom Organisme", "fields.Nom"],
    }

    return render(request, "annotation_v2/index.html", context)


def search_view(request: http.HttpRequest):
    """DataTables search endpoint.

    Ref : https://datatables.net/manual/server-side
    """

    offset = int(request.GET["start"])
    limit = int(request.GET["length"])
    unsafe_search_str = request.GET["search[value]"]

    initial_qs = Datalake.objects.filter(src_alias="mes_aides_aides")
    qs = initial_qs

    if unsafe_search_str:
        qs = qs.annotate(search=search.SearchVector("data")).filter(search=unsafe_search_str)

    qs = qs.values("id", "data")

    data = [{"DT_RowId": row_instance["id"], **row_instance["data"]} for row_instance in qs[offset : offset + limit]]

    return http.JsonResponse(
        data={
            "draw": request.GET["draw"],
            "recordsTotal": len(initial_qs),
            "recordsFiltered": len(qs),
            "data": data,
        }
    )
