# based on
# https://github.com/gip-inclusion/dora-back/blob/main/dora/admin_express/utils.py

CODES_ARRONDISSEMENTS_BY_CODE_COMMUNE = {
    # Paris
    "75056": [
        "75101",
        "75102",
        "75103",
        "75104",
        "75105",
        "75106",
        "75107",
        "75108",
        "75109",
        "75110",
        "75111",
        "75112",
        "75113",
        "75114",
        "75115",
        "75116",
        "75117",
        "75118",
        "75119",
        "75120",
    ],
    # Lyon
    "69123": [
        "69381",
        "69382",
        "69383",
        "69384",
        "69385",
        "69386",
        "69387",
        "69388",
        "69389",
    ],
    # Marseille
    "13055": [
        "13201",
        "13202",
        "13203",
        "13204",
        "13205",
        "13206",
        "13207",
        "13208",
        "13209",
        "13210",
        "13211",
        "13212",
        "13213",
        "13214",
        "13215",
        "13216",
    ],
}

CODE_COMMUNE_BY_CODE_ARRONDISSEMENT = {
    code_arrondissement: code_commune
    for code_commune, codes_arrondissements in CODES_ARRONDISSEMENTS_BY_CODE_COMMUNE.items()
    for code_arrondissement in codes_arrondissements
}
