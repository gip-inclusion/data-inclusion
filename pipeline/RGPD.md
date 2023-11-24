# Notification RGPD

Nous envoyons des notifications par courriel, chaque mois, aux utilisateurs dont nous avons eu connaissance dudit courriel
via l'une ou l'autre de nos sources, afin de leur proposer de s'opposer à la réutilisation de cette donnée dans
data⋅inclusion.

En l'absence de réponse de leur part, comme vu dans les CGAUs de data⋅inclusion, nous publierons le courriel en
question dans le jeu de données disponible dans l'API.

A date (fin 2023), nous ne proposons l'affichage que des courriels renseignés dans les structures et services DORA.

## Déroulement technique

### Imports Brevo
Chaque jour le DAG `import_brevo` récupère la liste à jour des personnes contactées lors des notifications RGPD.
Pour ce faire il réalise un extract + load (datalake puis datawarehouse) des données depuis l'API Brevo.

A noter que les contacts enregistrés dans Brevo sont identifiés par:
- leur email (qui est la clé unique de renseignement d'un contact Brevo)
- un ensemble d'attributs, dont:
  * la date d'opposition RGPD (remplie par Osiris)
  * la liste des structures dont l'adresse email est membre

### Traitements de données
Chaque jour le DAG `main` se chargera de créer les tables (staging & intermediate) liées d'un côté
aux courriels venant des nos sources (structures et services Dora uniquement pour l'instant) et
par ailleurs aux données de contact Brevo.

### Notification
Chaque mois, le DAG `notify_rgpd_contacts` tire une liste de contacts depuis les sources qui nous
intéressent (aujourd'hui, Dora uniquement) et en tire une liste de "nouveaux contacts".

Pour cela, il compare les `contact_uid` (combinaison de source, stream et UID) d'un contact enregistrés
dans Brevo et ceux provenant de Dora;
- si le `contact_uid` n'existe pas dans Brevo, ET que l'email n'existe pas non plus, c'est un nouveau contact.
- si le `contact_uid` n'existe pas dans Brevo, mais que l'email est connu, on va mettre à jour ce contact.

A noter que la mise à jour d'un contact va écraser les `contact_uids` par exemple, mais ne touchera pas à la
date d'opposition RGPD.

Dans tous les cas, si on a déjà connu ce `contact_uid` dans Brevo, quelle que soit l'adresse mail
associée, nous ne recontacterons pas.

Ainsi, une fois la liste des "nouveaux contacts" établie, le DAG se charge de:

1. Enregistrer dans Brevo la liste de tous les contacts ayant été contactés
2. Remettre à zéro et remplir dans Brevo la liste des "contacts à contacter ce mois-ci" 
3. Déclencher une nouvelle campagne de notification dans Brevo et l'envoyer

L'avantage d'utiliser des campagnes indépendantes mensuelles est que ces dernières permettent l'utilisation
des dashboards analytiques poussées de Brevo.

## Reste à faire

### Intégration dans l'API
Il faut intégrer les emails des sources Dora (et Emplois !) dans l'API.

Cela va concerner:

- toutes les adresses connues (notification envoyée à ce jour ou non)
- non "opposés" via le DPO dans Brevo
- non "unsubscribed"
- ipour le confort des utilisateurs, non hardbouncés bar Brevo : le mail est forcément invalide.
