WITH structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

labels_nationaux_seed AS (
    SELECT * FROM {{ ref('labels_nationaux') }}
),

final AS (
    SELECT
        id                                                                                                           AS "id",
        id                                                                                                           AS "adresse_id",
        siret                                                                                                        AS "siret",
        NULL                                                                                                         AS "rna",
        nom                                                                                                          AS "nom",
        telephone                                                                                                    AS "telephone",
        courriel                                                                                                     AS "courriel",
        site_web                                                                                                     AS "site_web",
        NULL                                                                                                         AS "lien_source",
        horaires_ouverture                                                                                           AS "horaires_ouverture",
        accessibilite                                                                                                AS "accessibilite",
        -- SOURCEFIX(2024-11-18) : The source probably won't be fix for a while. Mednum can't maintain the source
        -- correctly for now. We will keep the source as it is for now.
        CAST(ARRAY(
            SELECT labels_nationaux_seed.value AS labels FROM labels_nationaux_seed
            INTERSECT
            SELECT UNNEST(structures.labels_nationaux) AS labels
        ) AS TEXT [])                                                                                                AS "labels_nationaux",
        thematiques                                                                                                  AS "thematiques",
        -- FIXME(vperron) : Remove this when there is no 'PE' in the sources anymore
        CASE
            WHEN typologie = 'PE' THEN 'FT'
            ELSE typologie
        END                                                                                                          AS typologie,
        presentation_resume                                                                                          AS "presentation_resume",
        {{ truncate_text("presentation_detail") }} AS "presentation_detail",
        CAST(date_maj AS DATE)                                                                                       AS "date_maj",
        _di_source_id                                                                                                AS "source",
        labels_autres                                                                                                AS "labels_autres"
    FROM structures
)

SELECT * FROM final
