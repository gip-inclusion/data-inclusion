from typing import Optional

from furl import furl

from django import http
from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from annotation import services
from annotation.models import Annotation, Dataset
from data_inclusion.models import Structure
from sirene.models import CodeNAF
from sirene.services import get_naf_data_by_code


def check_and_get_dataset(dataset_slug: Optional[str]) -> Dataset:
    if dataset_slug is None:
        return http.HttpResponseBadRequest()

    try:
        dataset_instance = Dataset.objects.get(slug=dataset_slug)
    except Dataset.DoesNotExist:
        return http.HttpResponseNotFound()

    return dataset_instance


def check_permissions(user_instance, dataset_instance: Dataset):
    return user_instance.groups.filter(id=dataset_instance.organization.id).exists()


@login_required()
def index(request: http.HttpRequest):
    unsafe_dataset_slug_str = request.GET.get("dataset", None)

    dataset_instance = check_and_get_dataset(unsafe_dataset_slug_str)
    check_permissions(request.user, dataset_instance)

    return render(request, "annotation/index.html")


@login_required()
def partial_task(request: http.HttpRequest):
    if not request.htmx:
        return http.HttpResponseNotFound()

    unsafe_dataset_slug_str = furl(request.htmx.current_url).args.get("dataset")
    unsafe_code_insee_str = furl(request.htmx.current_url).args.get("code_insee")

    dataset_instance = check_and_get_dataset(unsafe_dataset_slug_str)
    check_permissions(request.user, dataset_instance)

    structure_instance_list = Structure.objects.raw(
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
                AND (structure.code_insee ~ ('^' || %(code_insee)s) OR structure._di_geocodage_code_insee ~ ('^' || %(code_insee)s))
            -- this allow concurrent users to work without too much overlap
            ORDER BY random()
            LIMIT 1""",  # noqa: E501
        {
            "dataset_source": dataset_instance.source,
            "code_insee": unsafe_code_insee_str or "",
        },
    )

    if len(structure_instance_list) == 0:
        return http.HttpResponse("Vous avez terminé ! :)")
    else:
        structure_instance = structure_instance_list[0]

    context = {
        "dataset_instance": dataset_instance,
        "structure_instance": structure_instance,
        "establishment_queryset": services.search_sirene(
            address=structure_instance.adresse,
            name=structure_instance.nom,
            code_insee=structure_instance.code_insee,
            siret=structure_instance.siret,
        ),
        "naf_section_list": [
            get_naf_data_by_code(level, code)
            for level, code in [
                (CodeNAF.Level.SECTION, "O"),
                (CodeNAF.Level.SECTION, "Q"),
                (CodeNAF.Level.SECTION, "G"),
            ]
        ],
        "naf_ape_queryset": CodeNAF.objects.exclude(code="00.00Z").all(),
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
    unsafe_code_insee = request.POST.get("code_insee", None)
    unsafe_siret = request.POST.get("siret", None)
    unsafe_naf_section_list = [
        value.split(",")[1] for value in request.POST.getlist("naf_section_list", []) if value != ""
    ]
    unsafe_naf_ape_list = [value for value in request.POST.getlist("naf_ape_list", []) if value != ""]
    unsafe_structure_type_list = [value for value in request.POST.getlist("structure_type_list", []) if value != ""]

    if isinstance(unsafe_structure_type_list, list):
        structure_types = {f"is_{structure_type_str}": True for structure_type_str in unsafe_structure_type_list}

    context = {
        "establishment_queryset": services.search_sirene(
            address=unsafe_address,
            name=unsafe_name,
            code_insee=unsafe_code_insee,
            siret=unsafe_siret,
            naf_section=unsafe_naf_section_list,
            naf_ape=unsafe_naf_ape_list,
            **structure_types,
        )
    }

    return render(request, "annotation/search.html", context)


@login_required()
def partial_submit(request: http.HttpRequest):
    if not request.htmx:
        return http.HttpResponseNotFound()

    unsafe_dataset_slug_str = furl(request.htmx.current_url).args.get("dataset")
    unsafe_structure_surrogate_id = request.POST.get("structure_surrogate_id", None)
    unsafe_skipped = request.POST.get("skipped", None)
    unsafe_closed = request.POST.get("closed", None)
    unsafe_irrelevant = request.POST.get("irrelevant", None)
    unsafe_is_parent = request.POST.get("is_parent", None)
    unsafe_siret = request.POST.get("siret", None)

    dataset_instance = check_and_get_dataset(unsafe_dataset_slug_str)
    check_permissions(request.user, dataset_instance)

    if not Structure.objects.filter(di_surrogate_id=unsafe_structure_surrogate_id).exists():
        return http.HttpResponseBadRequest()

    Annotation.objects.create(
        dataset_id=dataset_instance.id,
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
