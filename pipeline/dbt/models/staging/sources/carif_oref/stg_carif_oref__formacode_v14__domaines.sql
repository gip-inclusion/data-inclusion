SELECT formacode.*
FROM (
    VALUES
    ('11083', TRUE, 'Mathématiques mise à niveau'),
    ('15022', TRUE, 'Lutte contre l’illettrisme'),
    ('15030', TRUE, 'Calcul mise à niveau'),
    ('15031', TRUE, 'Adaptation sociale'),
    ('15040', TRUE, 'Français mise à niveau'),
    ('15041', TRUE, 'Mise à niveau'),
    ('15043', TRUE, 'Alphabétisation'),
    ('15050', TRUE, 'Mise à niveau numérique'),
    ('15061', TRUE, 'Accompagnement vers emploi'),
    ('15084', TRUE, 'Préparation entrée formation'),
    ('15235', TRUE, 'Français langue étrangère'),
    ('15281', TRUE, 'Français mise à niveau')
) AS formacode (code, selected, libelle)
