from dags.data_inclusion.pipeline.sources.france_travail import (
    format_date_ft_to_open_street_map,
)


def test_horaires_continus():
    horaires = [
        {
            "jour": 1,
            "horaireFerme": "N",
            "horaireEnContinu": "O",
            "ouvertureMatin": "08:00",
            "fermetureApresMidi": "18:00",
        }
    ]
    assert format_date_ft_to_open_street_map(horaires) == "Mo 08:00-18:00; PH off"


def test_horaires_non_continus_sans_apres_midi():
    horaires = [
        {
            "jour": 2,
            "horaireFerme": "N",
            "horaireEnContinu": "N",
            "ouvertureMatin": "09:00",
            "fermetureMatin": "12:00",
        }
    ]
    assert format_date_ft_to_open_street_map(horaires) == "Tu 09:00-12:00; PH off"


def test_deux_jours():
    horaires = [
        {
            "jour": 1,
            "horaireFerme": "N",
            "horaireEnContinu": "O",
            "ouvertureMatin": "08:00",
            "fermetureApresMidi": "18:00",
        },
        {
            "jour": 2,
            "horaireFerme": "N",
            "horaireEnContinu": "N",
            "ouvertureMatin": "09:00",
            "fermetureMatin": "12:00",
        },
    ]
    assert (
        format_date_ft_to_open_street_map(horaires)
        == "Mo 08:00-18:00;Tu 09:00-12:00; PH off"
    )


def test_horaires_non_continus_sans_matin():
    horaires = [
        {
            "jour": 3,
            "horaireFerme": "N",
            "horaireEnContinu": "N",
            "ouvertureApresMidi": "14:00",
            "fermetureApresMidi": "17:00",
        }
    ]
    assert format_date_ft_to_open_street_map(horaires) == "We 14:00-17:00; PH off"


def test_horaires_non_continus_complets():
    horaires = [
        {
            "jour": 4,
            "horaireFerme": "N",
            "horaireEnContinu": "N",
            "ouvertureMatin": "09:00",
            "fermetureMatin": "12:00",
            "ouvertureApresMidi": "14:00",
            "fermetureApresMidi": "17:00",
        }
    ]
    assert (
        format_date_ft_to_open_street_map(horaires)
        == "Th 09:00-12:00,14:00-17:00; PH off"
    )


def test_jour_ferme_seul():
    horaires = [{"jour": 4, "horaireFerme": "O"}]
    assert format_date_ft_to_open_street_map(horaires) is None


def test_jour_ferme():
    horaires = [
        {"jour": 4, "horaireFerme": "O"},
        {
            "jour": 5,
            "horaireFerme": "N",
            "horaireEnContinu": "N",
            "ouvertureMatin": "09:00",
            "fermetureMatin": "12:00",
        },
    ]
    assert format_date_ft_to_open_street_map(horaires) == "Fr 09:00-12:00; PH off"


def test_dictionnaire_vide():
    horaires = []
    assert format_date_ft_to_open_street_map(horaires) is None


def test_format_uniforme_tous_les_jours():
    horaires = [
        {
            "jour": 1,
            "horaireFerme": "N",
            "horaireEnContinu": "O",
            "ouvertureMatin": "08:00",
            "fermetureApresMidi": "18:00",
        },
        {
            "jour": 2,
            "horaireFerme": "N",
            "horaireEnContinu": "O",
            "ouvertureMatin": "08:00",
            "fermetureApresMidi": "18:00",
        },
        {
            "jour": 3,
            "horaireFerme": "N",
            "horaireEnContinu": "O",
            "ouvertureMatin": "08:00",
            "fermetureApresMidi": "18:00",
        },
        {
            "jour": 4,
            "horaireFerme": "N",
            "horaireEnContinu": "O",
            "ouvertureMatin": "08:00",
            "fermetureApresMidi": "18:00",
        },
        {
            "jour": 5,
            "horaireFerme": "N",
            "horaireEnContinu": "O",
            "ouvertureMatin": "08:00",
            "fermetureApresMidi": "18:00",
        },
    ]
    assert format_date_ft_to_open_street_map(horaires) == "Mo-Fr 08:00-18:00; PH off"


def test_horaires_vide():
    horaires = []
    assert format_date_ft_to_open_street_map(horaires) is None


def test_horaires_none():
    horaires = None
    assert format_date_ft_to_open_street_map(horaires) is None
