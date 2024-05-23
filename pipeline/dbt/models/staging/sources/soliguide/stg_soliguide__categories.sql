-- the mapping is documented here:
-- https://apisolidarite.soliguide.fr/Documentation-technique-de-l-API-Solidarit-ecaf8198f0e9400d93140b8043c9f2ce

-- noqa: disable=JJ01

SELECT
    x.code  AS "code",
    x.label AS "label"
FROM
    JSONB_EACH_TEXT(CAST($$
    {
        "access_to_housing": "Conseil logement",
        "accomodation_and_housing": "Hébergement et logement",
        "activities": "Activités",
        "acupuncture": "Acupuncture",
        "addiction": "Addiction",
        "administrative_assistance": "Conseil administratif",
        "allergology": "Allergologie",
        "animal_assitance": "Animaux",
        "baby_parcel": "Colis bébé",
        "babysitting": "Garde d'enfants",
        "budget_advice": "Conseil budget",
        "cardiology": "Cardiologie",
        "carpooling": "Co-voiturage",
        "chauffeur_driven_transport": "Transport avec chauffeur",
        "child_care": "Soins enfants",
        "citizen_housing": "Hébergement citoyen",
        "clothing": "Vêtements",
        "community_garden": "Jardin solidaire",
        "computers_at_your_disposal": "Ordinateur",
        "cooking_workshop": "Atelier cuisine",
        "counseling": "Conseil",
        "day_hosting": "Accueil de jour",
        "dental_care": "Dentaire",
        "dermatology": "Dermatologie",
        "digital_safe": "Coffre-fort numérique",
        "digital_tools_training": "Atelier numérique",
        "disability_advice": "Conseil handicap",
        "domiciliation": "Domiciliation",
        "echography": "Échographie",
        "electrical_outlets_available": "Prise",
        "emergency_accommodation": "Hébergement d'urgence",
        "endocrinology": "Endocrinologie",
        "equipment": "Matériel",
        "face_masks": "Masques",
        "family_area": "Espace famille",
        "food_distribution": "Distribution de repas",
        "food_packages": "Panier alimentaire",
        "food_voucher": "Bon / chèque alimentaire",
        "food": "Alimentation",
        "fountain": "Fontaine à eau",
        "french_course": "Cours de français",
        "gastroenterology": "Gastro-entérologie",
        "general_practitioner": "Médecin généraliste",
        "gynecology": "Gynécologie",
        "health_specialists": "Spécialistes",
        "health": "Santé",
        "hygiene_and_wellness": "Hygiène et bien-être",
        "hygiene_products": "Produits d'hygiène",
        "infirmary": "Infirmerie",
        "information_point": "Point d'information et d'orientation",
        "integration_through_economic_activity": "Insertion par l'activité économique",
        "job_coaching": "Accompagnement à l'emploi",
        "kinesitherapy": "Kinésithérapie",
        "laundry": "Laverie",
        "legal_advice": "Permanence juridique",
        "libraries": "Bibliothèque",
        "long_term_accomodation": "Hébergement à long terme",
        "luggage_storage": "Bagagerie",
        "mammography": "Mammographie",
        "mobility_assistance": "Aide à la mobilité",
        "mobility": "Transport / Mobilité",
        "museums": "Musée",
        "nutrition": "Nutrition",
        "ophthalmology": "Ophtalmologie",
        "osteopathy": "Ostéopathe",
        "other_activities": "Activités diverses",
        "otorhinolaryngology": "Oto-rhino-laryngologie",
        "overnight_stop": "Halte de nuit",
        "parent_assistance": "Conseil aux parents",
        "pedicure": "Pédicure",
        "phlebology": "Phlébologie",
        "pneumology": "Pneumologie",
        "pregnancy_care": "Suivi grossesse",
        "provision_of_vehicles": "Mise à disposition de véhicule",
        "psychological_support": "Psychologie",
        "public_writer": "Écrivain public",
        "radiology": "Radiologie",
        "rest_area": "Espace de repos",
        "rheumatology": "Rhumatologie",
        "seated_catering": "Restauration assise",
        "shared_kitchen": "Cuisine partagée",
        "shower": "Douche",
        "social_accompaniment": "Accompagnement social",
        "social_grocery_stores": "Épicerie Sociale et Solidaire",
        "solidarity_fridge": "Frigo solidaire",
        "solidarity_store": "Boutique solidaire",
        "speech_therapy": "Orthophonie",
        "sport_activities": "Activités sportives",
        "std_testing": "Dépistage",
        "stomatology": "Stomatologie",
        "technology": "Technologie",
        "telephone_at_your_disposal": "Téléphone",
        "toilets": "Toilettes",
        "training_and_jobs": "Formation et emploi",
        "tutoring": "Soutien scolaire",
        "urology": "Urologie",
        "vaccination": "Vaccination",
        "vet_care": "Vétérinaire",
        "welcome": "Accueil",
        "wellness": "Bien-être",
        "wifi": "Wifi"
    }
$$ AS JSONB)) AS x (code, label)
