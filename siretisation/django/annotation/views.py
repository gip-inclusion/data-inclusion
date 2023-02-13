from django import http
from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from annotation import services
from annotation.models import Annotation, Dataset
from data_inclusion.models import Structure
from sirene.models import CodeNAF
from sirene.services import get_naf_data_by_code


@login_required()
def index(request: http.HttpRequest):
    unsafe_dataset_slug_str = request.GET.get("dataset", None)

    try:
        dataset_instance = Dataset.objects.get(slug=unsafe_dataset_slug_str)
    except Dataset.DoesNotExist:
        return http.HttpResponseNotFound()

    if not request.user.groups.filter(id=dataset_instance.organization.id).exists():
        return http.HttpResponseForbidden()

    context = {
        "dataset_instance": dataset_instance,
    }

    return render(request, "annotation/index.html", context)


@login_required()
def partial_task(request: http.HttpRequest):
    if not request.htmx:
        return http.HttpResponseNotFound()

    unsafe_dataset_instance_id = request.GET.get("dataset_instance_id", None)

    try:
        dataset_instance = Dataset.objects.get(id=unsafe_dataset_instance_id)
    except Dataset.DoesNotExist:
        return http.HttpResponseNotFound()

    structure_instance = Structure.objects.raw(
        """
            WITH enhanced_annotation AS (
                SELECT
                    annotation_annotation.*,
                    annotation_dataset.source AS "source"
                FROM annotation_annotation
                INNER JOIN annotation_dataset ON annotation_annotation.dataset_id = annotation_dataset.id
            )
            SELECT
                structure.*
            FROM structure
            LEFT JOIN enhanced_annotation ON
                structure.source = enhanced_annotation.source
                AND structure._di_surrogate_id = enhanced_annotation.di_surrogate_id
            WHERE
                structure.source = %(dataset_source)s
                AND enhanced_annotation.id IS NULL
            -- this allow concurrent users to work without too much overlap
            ORDER BY random()
            LIMIT 1""",
        {
            "dataset_source": dataset_instance.source,
        },
    )[0]

    if structure_instance is None:
        return http.HttpResponse("Vous avez terminé ! :)")

    context = {
        "dataset_instance": dataset_instance,
        "structure_instance": structure_instance,
        "establishment_queryset": services.search_sirene(
            adresse=structure_instance.adresse,
            name=structure_instance.nom,
            postal_code=structure_instance.code_postal,
            siret=structure_instance.siret,
        ),
        "activity_list": [
            get_naf_data_by_code(level, code)
            for level, code in [
                (CodeNAF.Level.SECTION, "O"),
                (CodeNAF.Level.SECTION, "Q"),
                (CodeNAF.Level.DIVISION, "91"),
            ]
        ],
    }

    if dataset_instance.show_nearby_cnfs_permanences:
        context["permanence_queryset"] = services.search_cnfs(
            siret=structure_instance.siret,
            longitude=structure_instance.longitude,
            latitude=structure_instance.latitude,
        )

    return render(request, "annotation/task.html", context)


@login_required()
def partial_search(request: http.HttpRequest):
    if not request.htmx:
        return http.HttpResponseNotFound()

    unsafe_address = request.POST.get("adresse", None)
    unsafe_name = request.POST.get("nom", None)
    unsafe_postal_code = request.POST.get("code_postal", None)
    unsafe_siret = request.POST.get("siret", None)
    unsafe_naf_activities = [value.split(",") for value in request.POST.getlist("naf_activities", []) if value != ""]

    context = {
        "establishment_queryset": services.search_sirene(
            adresse=unsafe_address,
            name=unsafe_name,
            postal_code=unsafe_postal_code,
            siret=unsafe_siret,
            naf_activities=unsafe_naf_activities,
        )
    }

    return render(request, "annotation/search.html", context)


@login_required()
def partial_submit(request: http.HttpRequest):
    if not request.htmx:
        return http.HttpResponseNotFound()

    unsafe_dataset_instance_id = request.POST.get("dataset_instance_id", None)
    unsafe_structure_surrogate_id = request.POST.get("structure_surrogate_id", None)
    unsafe_skipped = request.POST.get("skipped", None)
    unsafe_closed = request.POST.get("closed", None)
    unsafe_irrelevant = request.POST.get("irrelevant", None)
    unsafe_is_parent = request.POST.get("is_parent", None)
    unsafe_siret = request.POST.get("siret", None)

    if not Dataset.objects.filter(id=unsafe_dataset_instance_id).exists():
        return http.HttpResponseBadRequest()

    if not Structure.objects.filter(di_surrogate_id=unsafe_structure_surrogate_id).exists():
        return http.HttpResponseBadRequest()

    Annotation.objects.create(
        dataset_id=unsafe_dataset_instance_id,
        di_surrogate_id=unsafe_structure_surrogate_id,
        skipped=bool(unsafe_skipped),
        closed=bool(unsafe_closed),
        irrelevant=bool(unsafe_irrelevant),
        is_parent=bool(unsafe_is_parent),
        siret=unsafe_siret if unsafe_siret is not None else "",
        created_by=request.user,
    )

    context = {
        "skipped": unsafe_skipped,
        "irrelevant": unsafe_irrelevant,
        "is_parent": unsafe_is_parent,
        "closed": unsafe_closed,
    }

    return render(request, "annotation/submit.html", context)
