BEGIN;

DROP TABLE IF EXISTS sirene_etablissement_searchable_name;

CREATE TABLE sirene_etablissement_searchable_name AS
SELECT
    sirene_etablissement_geocode.siret,
    COALESCE(
        sirene_stock_unite_legale."denominationUniteLegale", ''
    )
    || COALESCE(
        ' ' || sirene_stock_unite_legale."sigleUniteLegale", ''
    )
    ||
    COALESCE(
        ' '
        ||
        CASE
            WHEN
                NOT(
                    sirene_stock_unite_legale."denominationUniteLegale"
                    LIKE (
                        sirene_etablissement_geocode."denominationUsuelleEtablissement" -- noqa: L016
                        || '%'
                    )
                )
                THEN sirene_etablissement_geocode."denominationUsuelleEtablissement" -- noqa: L016
            ELSE ''
        END, '')
    || COALESCE(
        ' ' || sirene_etablissement_geocode."enseigne1Etablissement", ''
    )
    || COALESCE(
        ' ' || sirene_etablissement_geocode."enseigne2Etablissement", ''
    ) AS "searchable_name"
FROM
    sirene_etablissement_geocode
INNER JOIN sirene_stock_unite_legale
    ON LEFT(sirene_etablissement_geocode.siret, 9)
        = sirene_stock_unite_legale.siren
WHERE
    sirene_stock_unite_legale."categorieJuridiqueUniteLegale" != 1000;

-- Use CASCADE to prevent incoherences when first updating etablissements
ALTER TABLE sirene_etablissement_searchable_name
ADD FOREIGN KEY (siret) REFERENCES sirene_etablissement_geocode (siret)
ON DELETE CASCADE;

CREATE INDEX sirene_etablissement_searchable_name_trgm_idx
ON sirene_etablissement_searchable_name
USING GIN(searchable_name gin_trgm_ops);

COMMIT;
