# Notification RGPD

Nous envoyons des notifications par courriel, chaque mois, aux utilisateurs dont nous avons eu connaissance dudit courriel
via l'une ou l'autre de nos sources, afin de leur proposer de s'opposer à la réutilisation de cette donnée dans
data⋅inclusion.

En l'absence de réponse de leur part, comme vu dans les CGAUs de data⋅inclusion, nous publierons le courriel en
question dans le jeu de données disponible dans l'API.

L'intérêt secondaire pour data⋅inclusion de ces notification est de permettre une validation poussée des e-mails de contact
ainsi notifiés; nous savons donc pour chaque courriel, s'il est digne de confiance ou non.

## Déroulement technique

### Imports Brevo
Chaque jour le DAG `import_brevo` récupère la liste à jour des personnes contactées lors des notifications RGPD.
Pour ce faire il réalise un extract + load (datalake puis datawarehouse) des données depuis l'API Brevo.

A noter que les contacts enregistrés dans Brevo contiennent a minima:
- leur email (qui est la clé unique de renseignement d'un contact Brevo)
- le fait que cet email ait donné lieu à un envoi ou non, avec ou sans 'hardbounce'
- un ensemble d'attributs, dont la date d'opposition RGPD (remplie par Osiris)

### Traitements de données
Chaque jour le DAG `main` se chargera de créer la table `int__union_contacts__enhanced` qui relie courriels venant
de nos sources de données, et courriels connus de Brevo (avec succès de l'envoi ou non, date d'opposition...)

### Notification
Chaque mois, le DAG `notify_rgpd_contacts` tire une liste de courriels connus depuis nos sources.

Ceux n'ayant jamais fait l'objet d'une notification seront déposés dans une liste qui servira à l'envoi
de la prochaine notification; tous les contacts sont par ailleurs ajoutés à la liste de "tous les contacts
connus", qui est celle importée régulièrement par `import_brevo`.

Puis nous déclenchons l'envoi d'une nouvelle campagne de notification RGPD pour le mois courant, envoyée
à la liste des "nouveaux emails" connus.

Par le passé nous maintenions une liste des structures et services ayant été liés à un email, grâce à un attribut
spécial sous forme de liste, attaché à chaque contact et envoyé à Brevo. Nous avons remarqué au bout d'un an,
que aucune source ne modifiait ses courriels; nous avonc donc cessé de maintenir ce système complexe, mais il en
reste des traces dans certains contacts Brevo. Ils ne sont plus mis à jour.
