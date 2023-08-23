WITH source AS (
    SELECT * FROM {{ source('agefiph', 'services') }}
),


final AS (
    SELECT
        _di_source_id                                                     AS "_di_source_id",
        data ->> 'id'                                                     AS "id",
        CAST(data #>> '{attributes,created}' AS TIMESTAMP WITH TIME ZONE) AS "attributes__created",
        CAST(data #>> '{attributes,changed}' AS TIMESTAMP WITH TIME ZONE) AS "attributes__changed",
        data #>> '{attributes,title}'                                     AS "attributes__title",
        data #>> '{attributes,field_titre_card_employeur}'                AS "attributes__field_titre_card_employeur",
        data #>> '{attributes,field_essentiel_ph,processed}'              AS "attributes__field_essentiel_ph__processed",
        data #>> '{attributes,field_essentiel_employeur,processed}'       AS "attributes__field_essentiel_employeur__processed",
        data #>> '{attributes,field_texte_brut_long}'                     AS "attributes__field_texte_brut_long",
        data #>> '{attributes,path,alias}'                                AS "attributes__path__alias",
        data #>> '{relationships.field_type_aide_service.data.id}'        AS "relationships__field_type_aide_service__data__id"
    FROM source
)

SELECT * FROM final
