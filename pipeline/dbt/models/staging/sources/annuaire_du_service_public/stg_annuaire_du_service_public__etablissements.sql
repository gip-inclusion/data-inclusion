WITH source AS (
    {{ stg_source_header('annuaire_du_service_public', 'etablissements') }}
),

final AS (
    SELECT
        CAST(
            ARRAY(
                SELECT elem.value
                FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'adresse_courriel') AS elem (value)
            ) AS TEXT[]
        )                                    AS "adresse_courriel",
        source.data ->> 'id'                 AS "id",
        source.data ->> 'siret'              AS "siret",
        source.data ->> 'siren'              AS "siren",
        source.data ->> 'nom'                AS "nom",
        source.data ->> 'nom_commune'        AS "nom_commune",
        source.data ->> 'code_postal'        AS "code_postal",
        source.data ->> 'code_insee_commune' AS "code_insee_commune",
        source.data ->> 'numero_voie'        AS "numero_voie",
        source.data ->> 'complement1'        AS "complement1",
        source.data ->> 'longitude'          AS "longitude",
        source.data ->> 'latitude'           AS "latitude",
        source.data ->> 'type_service_local' AS "type_service_local",
        source.data ->> 'mission'            AS "mission",
        source.data ->> 'date_modification'  AS "date_modification",
        source.data ->> 'partenaire'         AS "partenaire"
    FROM source
)

SELECT * FROM final
