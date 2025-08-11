WITH source AS (
    {{ stg_source_header('agefiph', 'services') }}
),

final AS (
    SELECT
        data ->> 'id'                                                     AS "id",
        CAST(data #>> '{attributes,created}' AS TIMESTAMP WITH TIME ZONE) AS "attributes__created",
        CAST(data #>> '{attributes,changed}' AS TIMESTAMP WITH TIME ZONE) AS "attributes__changed",
        data #>> '{attributes,title}'                                     AS "attributes__title",
        data #>> '{attributes,path,alias}'                                AS "attributes__path__alias",
        data #>> '{attributes,field_lien_aide,uri}'                       AS "attributes__field_lien_aide_uri",
        data #>> '{relationships,field_profil_associe,data,id}'           AS "relationships__field_profil_associe__data__id",
        data #>> '{attributes,field_solution_detail,processed}'           AS "attributes__field_solution_detail__processed",
        data #>> '{attributes,field_montant_aide}'                        AS "attributes__field_montant_aide",
        data #>> '{attributes,field_solution_partenaire}'                 AS "attributes__field_solution_partenaire",
        data #>> '{relationships,field_type_de_solution,data,id}'         AS "relationships__field_type_de_solution__data__id"
    FROM source
)

SELECT * FROM final
