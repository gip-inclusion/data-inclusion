BEGIN;

DROP TABLE IF EXISTS sirene_etablissement_searchable_l4;

CREATE TABLE sirene_etablissement_searchable_l4 AS
SELECT
    sirene_etablissement_geocode."siret",
    COALESCE(sirene_etablissement_geocode."geo_l4",
        COALESCE(sirene_etablissement_geocode."numeroVoieEtablissement", '')
        || COALESCE(
            sirene_etablissement_geocode."indiceRepetitionEtablissement", ''
        )
        || COALESCE(
            ' ' || sirene_etablissement_geocode."typeVoieEtablissement", ''
        )
        || COALESCE(
            ' ' || sirene_etablissement_geocode."libelleVoieEtablissement", ''
        )
    ) AS "searchable_l4"
FROM
    sirene_etablissement_geocode
INNER JOIN sirene_stock_unite_legale
    ON LEFT(sirene_etablissement_geocode.siret, 9)
        = sirene_stock_unite_legale.siren
WHERE
    sirene_stock_unite_legale."categorieJuridiqueUniteLegale" != 1000;

-- Use CASCADE to prevent incoherences when first updating etablissements
ALTER TABLE sirene_etablissement_searchable_l4
ADD FOREIGN KEY (siret) REFERENCES sirene_etablissement_geocode (siret)
ON DELETE CASCADE;

CREATE INDEX sirene_etablissement_searchable_l4_trgm_idx
ON sirene_etablissement_searchable_l4
USING GIN(searchable_l4 gin_trgm_ops);

COMMIT;
