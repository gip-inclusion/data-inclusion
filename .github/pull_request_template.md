[Lien vers le ticket notion associé]()

### Check-list

* [ ] Mes commits et ma PR suivent le [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/)
  * `<type>(<optional scope>): <description>`
  * types : `feat`, `chore`, `fix`, `docs`, etc.
  * scopes : `api`, `pipeline`, `deployment`, `deduplication`, `datawarehouse`
* [ ] Mes messages de commit sont en anglais
* [ ] J'ai exécuté les pre-commits
* [ ] J'ai indiqué le ticket notion associé
* [ ] J'ai passé le ticket notion en review


si ma PR concerne la **pipeline**, le **datawarehouse** ou la **deduplication**

* [ ] J'ai actualisé les données liées à ma PR (avec une extraction récente)
* [ ] J'ai utilisé un dump récent de la prod
* [ ] J'ai exécuté en local les modèles (via `dbt build -s models/...` ou airflow en local)
* [ ] J'ai déployé en staging et exécuté les dags
* [ ] J'ai indiqué quelques questions metabase pour mettre en évidence l'impact de ma PR

si ma PR concerne l'**api**

* [ ] J'ai réimporté les données marts depuis le datalake
* [ ] J'emploie le français dans l'interface de l'api (query params, description, etc.)
* [ ] Si ma PR inclut des modifs de l'orm, j'ai inclus les migrations alembic
* [ ] J'ai ajouté des tests
* [ ] J'ai fait une analyse perfs avant/après via locust
