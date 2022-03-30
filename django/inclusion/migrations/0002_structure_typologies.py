from django.db import migrations

STRUCTURE_TYPOLOGIES = [
    ("ACI", "Structures porteuses d’ateliers et chantiers d’insertion (ACI)"),
    ("ACIPHC", "SIAE — Atelier chantier d’insertion premières heures en chantier"),
    ("AFPA", "Agence nationale pour la formation professionnelle des adultes (AFPA)"),
    ("AI", "Associations intermédiaires (AI)"),
    ("ASE", "Aide sociale à l’enfance (ASE)"),
    ("ASSO", "Associations"),
    ("CADA", "Centres d’accueil de demandeurs d’asile (CADA)"),
    ("CAF", "Caisses d’allocation familiale (CAF)"),
    ("CAP_EMPLOI", "Cap Emploi"),
    ("CAVA", "Centres d’adaptation à la vie active (CAVA)"),
    ("CC", "Communautés de Commune"),
    ("CCAS", "Centres communaux d’action sociale (CCAS)"),
    ("CD", "Conseils Départementaux (CD)"),
    ("CHRS", "Centres d’hébergement et de réinsertion sociale (CHRS)"),
    ("CHU", "Centres d’hébergement d’urgence (CHU)"),
    ("CIAS", "Centres intercommunaux d’action sociale (CIAS)"),
    ("CIDFF", "Centres d’information sur les droits des femmes et des familles (CIDFF)"),
    ("CPH", "Centres provisoires d’hébergement (CPH)"),
    ("CS", "Centre social"),
    ("CT", "Collectivités territoriales"),
    ("DEETS", "Directions de l’Economie, de l’Emploi, du Travail et des Solidarités (DEETS)"),
    ("DIPLP", "Délégation interministérielles à la prévention et à la lutte contre la pauvreté"),
    ("EA", "Entreprise adaptée (EA)"),
    ("EATT", "Entreprise Adaptée (EATT)"),
    ("EI", "Entreprises d’insertion (EI)"),
    ("EITI", "Entreprises d’insertion par le travail indépendant (EITI)"),
    ("EPCI", "Intercommunalité (EPCI)"),
    ("ETTI", "Entreprises de travail temporaire d’insertion (ETTI)"),
    ("FAIS", "Fédérations d’acteurs de l’insertion et de la solidarité"),
    ("GEIQ", "Groupements d’employeurs pour l’insertion et la qualification (GEIQ)"),
    ("ML", "Mission Locale"),
    ("MQ", "Maison de quartier"),
    ("MSA", "Mutualité Sociale Agricole"),
    ("MSAP", "Maison de Service au Public (MSAP)"),
    ("MUNI", "Municipalités"),
    ("OACAS", "Structures agréées Organisme d’accueil communautaire et d’activité solidaire (OACAS)"),
    ("OF", "Organisme de formations"),
    ("PE", "Pôle emploi"),
    ("PIJ_BIJ", "Points et bureaux information jeunesse (PIJ/BIJ)"),
    ("PIMMS", "Point Information Médiation Multi Services (PIMMS)"),
    ("PJJ", "Protection judiciaire de la jeunesse (PJJ)"),
    ("PLIE", "Plans locaux pour l’insertion et l’emploi (PLIE)"),
    ("SCP", "Services et clubs de prévention"),
    ("SPIP", "Services pénitentiaires d’insertion et de probation (SPIP)"),
    ("UDAF", "Union Départementale d’Aide aux Familles (UDAF)"),
    ("ASSO_CHOMEUR", "Associations de chômeurs"),
    ("Autre", "Autre"),
    ("PREF", "Préfecture, Sous-Préfecture"),
    ("REG", "Région"),
    ("DEPT", "Services sociaux du Conseil départemental"),
    ("TIERS_LIEUX", "Tiers lieu & coworking"),
    ("CAARUD", "CAARUD - Centre d'accueil et d'accompagnement à la réduction de risques pour usagers de drogues"),
    ("CSAPA", "CSAPA - Centre de soins, d'accompagnement et de prévention en addictologie"),
    ("E2C", "E2C - École de la deuxième chance"),
    ("EPIDE", "EPIDE - Établissement pour l'insertion dans l'emploi"),
    ("HUDA", "HUDA - Hébergement d'urgence pour demandeurs d'asile"),
    ("ODC", "Organisation délégataire d'un CD"),
    ("OIL", "Opérateur d'intermédiation locative"),
    ("OPCS", "Organisation porteuse de la clause sociale"),
    ("PENSION", "Pension de famille / résidence accueil"),
    ("PREVENTION", "Service ou club de prévention"),
    ("RS_FJT", "Résidence sociale / FJT - Foyer de Jeunes Travailleurs"),
]


def structure_typologies_fixture(apps, _):
    StructureTypology = apps.get_model("inclusion", "StructureTypology")
    for value, label in STRUCTURE_TYPOLOGIES:
        StructureTypology.objects.create(value=value, label=label)


class Migration(migrations.Migration):
    dependencies = [
        ("inclusion", "0001_initial"),
    ]

    operations = [migrations.RunPython(structure_typologies_fixture, migrations.RunPython.noop)]
