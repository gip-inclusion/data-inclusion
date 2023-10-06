{% set source_model = source('annuaire_du_service_public', 'etablissements') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

WITH source AS (
    SELECT * FROM {{ source_model }}
),

{% else %}

    WITH source AS (
        SELECT
            NULL                AS "_di_source_id",
            CAST(NULL AS JSONB) AS "data"
        WHERE FALSE
    ),

{% endif %}

final AS (
    SELECT
        _di_source_id                                                                               AS "_di_source_id",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'adresse_courriel')) AS TEXT []) AS "adresse_courriel",
        data ->> 'id'                                                                               AS "id",
        data ->> 'siret'                                                                            AS "siret",
        data ->> 'siren'                                                                            AS "siren",
        data ->> 'nom'                                                                              AS "nom",
        data ->> 'nom_commune'                                                                      AS "nom_commune",
        data ->> 'code_postal'                                                                      AS "code_postal",
        data ->> 'code_insee_commune'                                                               AS "code_insee_commune",
        data ->> 'numero_voie'                                                                      AS "numero_voie",
        data ->> 'complement1'                                                                      AS "complement1",
        data ->> 'longitude'                                                                        AS "longitude",
        data ->> 'latitude'                                                                         AS "latitude",
        data ->> 'type_service_local'                                                               AS "type_service_local",
        data ->> 'mission'                                                                          AS "mission",
        data ->> 'date_modification'                                                                AS "date_modification",
        data ->> 'partenaire'                                                                       AS "partenaire"
    FROM source
)

SELECT * FROM final
