WITH source AS (
    {{ stg_source_header('agefiph', 'structures') }}
),

final AS (
    SELECT
        _di_source_id                                                     AS "_di_source_id",
        data ->> 'id'                                                     AS "id",
        data #>> '{attributes,title}'                                     AS "attributes__title",
        CAST(data #>> '{attributes,created}' AS TIMESTAMP WITH TIME ZONE) AS "attributes__created",
        CAST(data #>> '{attributes,changed}' AS TIMESTAMP WITH TIME ZONE) AS "attributes__changed",
        data #>> '{attributes,field_adresse,locality}'                    AS "attributes__field_adresse__locality",
        data #>> '{attributes,field_adresse,postal_code}'                 AS "attributes__field_adresse__postal_code",
        data #>> '{attributes,field_adresse,address_line1}'               AS "attributes__field_adresse__address_line1",
        data #>> '{attributes,field_adresse,address_line2}'               AS "attributes__field_adresse__address_line2",
        data #>> '{attributes,field_courriel}'                            AS "attributes__field_courriel",
        CAST(data #>> '{attributes,field_geolocalisation,lat}' AS FLOAT)  AS "attributes__field_geolocalisation__lat",
        CAST(data #>> '{attributes,field_geolocalisation,lng}' AS FLOAT)  AS "attributes__field_geolocalisation__lng",
        data #>> '{attributes,field_texte_brut_long}'                     AS "attributes__field_texte_brut_long",
        data #>> '{attributes,field_telephone}'                           AS "attributes__field_telephone",
        data #>> '{attributes,field_lien_externe,uri}'                    AS "attributes__field_lien_externe__uri"
    FROM source
    -- Filter to retrieve exclude partner and retrieve only regional offices
    WHERE data #>> '{relationships,field_type_contact,data,id}' = '3050a487-098f-4af0-9809-bc7ec4a5b999' -- noqa: ST10
)

SELECT * FROM final
