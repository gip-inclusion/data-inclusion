SELECT
    'agefiph'                             AS "source",
    'agefiph--' || x.agefiph_structure_id AS "id",
    x.*
FROM (
    VALUES
    -- noqa: disable=layout.spacing
    ('6b91f968-b847-409f-95d3-6f6ac5754fa1', 'Amiens',                '80048', '80021', '3 rue Vincent Auriol',                   'CS 64801',                                        2.303842,  49.889643),
    ('32264b21-5a99-4b55-a01e-5eda6eec58bc', 'Nantes',                '44032', '44109', '34 quai Magellan',                       NULL,                                             -1.544621,  47.210388),
    ('dae475c5-fc7b-4a5b-be96-a8f3d37aadaf', 'Rouen',                 '76107', '76540', '30 rue Gadeau de Kerville',              'Immeuble Les Galées du Roi',                      1.081291,  49.430067),
    ('313c5ed5-2dd3-4850-86a8-3b946c08cb4c', 'Lille',                 '59040', '59350', '27 bis rue du Vieux Faubourg',           '3ième étage',                                     3.070053,  50.638241),
    ('2a0590e9-6c5d-4a5f-93ad-926df0e76c16', 'Toulouse',              '31505', '31555', '17 Boulevard De La Gare',                'BP 95827, Immeuble La Passerelle St Aubin',      55.290405, -20.944071),
    ('bc77b620-a0ea-4a95-8245-a69fc9ac4f3a', 'Limoges',               '87008', '87085', '3 Cours Gay Lussac',                     'Immeuble Manager 2 - CS 50 297',                  1.261256,  45.835280),
    ('52c4c68b-33d3-4ff1-916c-28c835025597', 'Montpellier',           '34967', '34172', '119 Avenue Jacques Cartier',             'Immeuble Antalya - Zac Antigone - CS 19008',      3.892147,  43.607057),
    ('77ba7123-00d7-47c7-9397-cecfbafadf9e', 'Sainte-Clotilde',       '97495', '97411', '62 bd du Chaudron',                      'Centre d''Affaires Cadjee - Bât C - 2ème Etage', 55.496422, -20.897877),
    ('64eacd54-6111-4089-85dd-b18fe6aa84df', 'Arcueil',               '94110', '94003', '24/28 Villa Baudran',                    '21/37 rue de Stalingrad - Immeuble Le Baudran',   2.337229,  48.808860),
    ('693c8a1f-19a7-4a0d-af9a-86a5e2c25fb7', 'Reims',                 '51100', '51454', '5, rue du Président Franklin Roosevelt', 'Immeuble Le Roosevelt - 2ème étage',              4.026390,  49.262351),
    ('e59058d7-254e-4083-95fb-8506e96ee824', 'Orléans',               '45058', '45234', '35 Avenue de Paris',                     'ABC2',                                            1.903990,  47.909886),
    ('b560abeb-47fe-4354-825c-a7061d2b5746', 'Rennes',                '35000', '35238', '4 avenue Charles et Raymonde Tillon',    NULL,                                             -1.687604,  48.127350),
    ('c1179739-1bd7-450f-9b15-50653442ff05', 'Dijon',                 '21066', '21231', '7 Boulevard Winston Churchill',          'Immeuble Osiris BP 66615',                        5.052879,  47.352692),
    ('1cf38c63-1c5d-49d9-9a39-819376029870', 'Bordeaux',              '33072', '33063', '13 rue Jean-Paul Alaux',                 'Millenium 2 ZAC Coeur de Bastide - CS 61404',    -0.563468,  44.843598),
    ('b61ca1a2-f723-4890-bd96-f427378e274b', 'Fort-De-France',        '97200', '97209', '2, Avenue des Arawaks',                  'Immeuble Eole 1',                               -61.046712,  14.616709),
    ('7263ac35-0409-42e6-a649-9a38ecb9d7cb', 'L''Isle-d''Abeau',      '38080', '38193', '33 Rue Saint Théobald',                  'Parc D''Affaires De Saint-Hubert',                5.219073,  45.615076),
    ('8ca5f3c1-e52e-46f1-b33c-8b65f8ec26f9', 'Meyreuil',              '13590', '13060', '1 rue de la Carrière de Bachasson',      'Arteparc de Bachasson bâtiment B',                5.530711,  43.491310),
    ('46c568b3-60b2-40bd-8475-5f4fd4d78986', 'Poitiers',              '86035', '86194', '14 Boulevard Chasseigne',                'Capitole V',                                      0.344430,  46.590367),
    ('8a50f97f-2852-462b-ac9c-d1afb9c63ebd', 'Nancy',                 '54063', '54395', '13-15 Boulevard Joffre',                 'Immeuble Joffre Saint Thiebault',                 6.178433,  48.687776),
    ('10387caf-d41b-4c14-bc3f-dc8e34d41dce', 'Clermont-Ferrand',      '63000', '63113', '65, boulevard François Mitterand',       'CS 70357',                                        3.086252,  45.770594)
    -- noqa: enable=layout.spacing
) AS x (agefiph_structure_id, commune, code_postal, code_insee, adresse, complement_adresse, longitude, latitude)
