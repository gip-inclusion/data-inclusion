from django_htmx.http import HttpResponseClientRedirect
from furl import furl

from django import http, shortcuts, urls
from django.contrib.auth.decorators import login_required
from django.contrib.postgres import search

from matching import models


@login_required()
def index(request: http.HttpRequest):
    unsafe_left_datasource_slug = request.GET.get("left_datasource_slug", None)
    unsafe_left_stream_slug = request.GET.get("left_stream_slug", None)
    unsafe_right_datasource_slug = request.GET.get("right_datasource_slug", None)
    unsafe_right_stream_slug = request.GET.get("right_stream_slug", None)
    unsafe_row_natural_id = request.GET.get("left_row_natural_id", None)

    left_datasource_instance = models.Datasource.objects.filter(slug=unsafe_left_datasource_slug).first()
    if left_datasource_instance is None:
        return http.HttpResponseBadRequest()

    left_stream_instance = models.Stream.objects.filter(slug=unsafe_left_stream_slug).first()
    if left_stream_instance is None:
        return http.HttpResponseBadRequest()

    right_datasource_instance = models.Datasource.objects.filter(slug=unsafe_right_datasource_slug).first()
    if right_datasource_instance is None:
        return http.HttpResponseBadRequest()

    right_stream_instance = models.Stream.objects.filter(slug=unsafe_right_stream_slug).first()
    if right_stream_instance is None:
        return http.HttpResponseBadRequest()

    if unsafe_row_natural_id is None:
        # select next row instance
        # exclude annotated rows
        datalake_qs = models.Datalake.objects.raw(
            """
            WITH enhanced_matching AS (
                SELECT
                    matching_matching.*,
                    matching_stream.name AS "stream_name",
                    matching_datasource.name AS "datasource_name"
                FROM matching_matching
                INNER JOIN matching_stream ON matching_matching.left_stream_id = matching_stream.id
                INNER JOIN matching_datasource ON matching_stream.datasource_id = matching_datasource.id
            )
            SELECT
                datalake.*
            FROM datalake
            LEFT JOIN enhanced_matching ON
                datalake.src_alias = enhanced_matching.datasource_name
                AND datalake.file ~ enhanced_matching.stream_name
                AND datalake.data_normalized->>'id' = enhanced_matching.left_row_natural_id::TEXT
            WHERE
                datalake.src_alias = %(left_datasource_name)s
                AND datalake.file ~ %(left_stream_name)s
                AND enhanced_matching.id IS NULL
            -- this allow concurrent users to work without too much overlap
            ORDER BY random()
            LIMIT 1""",
            {
                "left_datasource_name": left_datasource_instance.name,
                "left_stream_name": left_stream_instance.name,
            },
        )
        left_row_instance = next((row for row in datalake_qs), None)
    else:
        # user has provided a natural id, use it
        left_row_instance = models.Datalake.objects.filter(
            src_alias=left_datasource_instance.name,
            file__contains=left_stream_instance.name,
            data_normalized__id=unsafe_row_natural_id,
        ).first()

    context = {
        "left_stream_instance": left_stream_instance,
        "right_stream_instance": right_stream_instance,
        "left_row_instance": left_row_instance,
        "datatable_column_name_list": right_stream_instance.selected_columns,
    }

    return shortcuts.render(request, "matching/index.html", context)


@login_required()
def search_view(request: http.HttpRequest):
    """DataTables search endpoint.

    Ref : https://datatables.net/manual/server-side
    """

    offset = int(request.GET["start"])
    limit = int(request.GET["length"])
    unsafe_search_str = request.GET["search[value]"]
    unsafe_right_stream_id = request.GET.get("right_stream_id", None)

    right_stream_instance = models.Stream.objects.filter(id=unsafe_right_stream_id).first()
    if right_stream_instance is None:
        return http.HttpResponseBadRequest()

    initial_qs = models.Datalake.objects.filter(
        src_alias=right_stream_instance.datasource.name,
        file__contains=right_stream_instance.name,
    )

    qs = initial_qs
    if unsafe_search_str:
        qs = qs.annotate(search=search.SearchVector("data")).filter(search=unsafe_search_str)
    qs = qs.values("id", "data")
    qs = qs.order_by("id")

    data = [{"DT_RowId": row_instance["id"], **row_instance["data"]} for row_instance in qs[offset : offset + limit]]

    return http.JsonResponse(
        data={
            "draw": request.GET["draw"],
            "recordsTotal": len(initial_qs),
            "recordsFiltered": len(qs),
            "data": data,
        }
    )


@login_required()
def partial_matching(request: http.HttpRequest):
    if not request.htmx:
        return http.HttpResponseNotFound()

    unsafe_left_stream_id = request.POST.get("left_stream_id", None)
    unsafe_right_stream_id = request.POST.get("right_stream_id", None)
    unsafe_left_row_id = request.POST.get("left_row_id", None)
    unsafe_right_row_id_list = request.POST.getlist("right_row_id_list", [])
    unsafe_skipped = request.POST.get("skipped", None)
    unsafe_no_matching_row = request.POST.get("no_matching_row", None)

    left_stream_instance = models.Stream.objects.filter(id=unsafe_left_stream_id).first()
    if left_stream_instance is None:
        return http.HttpResponseBadRequest()

    right_stream_instance = models.Stream.objects.filter(id=unsafe_right_stream_id).first()
    if right_stream_instance is None:
        return http.HttpResponseBadRequest()

    left_row_instance = models.Datalake.objects.filter(
        id=unsafe_left_row_id,
        src_alias=left_stream_instance.datasource.name,
    ).first()
    if left_row_instance is None:
        return http.HttpResponseBadRequest()

    try:
        right_row_instance_list = [
            models.Datalake.objects.get(
                id=row_id,
                src_alias=right_stream_instance.datasource.name,
            )
            for row_id in unsafe_right_row_id_list
        ]
    except models.Datalake.DoesNotExist:
        return http.HttpResponseBadRequest()

    base_matching_data = {
        "left_stream": left_stream_instance,
        "right_stream": right_stream_instance,
        "left_row_natural_id": left_row_instance.data_normalized["id"],
        "left_row_data": left_row_instance.data,
        "logical_date": left_row_instance.logical_date,
        "created_by": request.user,
    }

    if bool(unsafe_skipped):
        models.Matching.objects.create(**base_matching_data, skipped=True)
    elif bool(unsafe_no_matching_row):
        models.Matching.objects.create(**base_matching_data, no_matching_row=True)
    else:
        for right_row_instance in right_row_instance_list:
            models.Matching.objects.create(
                **base_matching_data,
                right_row_natural_id=right_row_instance.data_normalized["id"],
                right_row_data=right_row_instance.data,
            )

    url = furl(urls.reverse("matching"))
    url.set(
        {
            "left_datasource_slug": left_stream_instance.datasource.slug,
            "left_stream_slug": left_stream_instance.slug,
            "right_datasource_slug": right_stream_instance.datasource.slug,
            "right_stream_slug": right_stream_instance.slug,
        }
    )
    return HttpResponseClientRedirect(str(url))
