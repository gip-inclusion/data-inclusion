# `data-inclusion-analyse`

Analyses des jeux de données des partenaires de data.inclusion

## Analyses

| Partenaire      | Jeu de données           | Date       | Notebook                                                     |
| --------------- | ------------------------ | ---------- | ------------------------------------------------------------ |
| 1jeune1solution | benefits                 | (api)      | [Notebook](./notebooks/1j1s/benefits.ipynb)                  |
| cd35            | annuaire social          | (api)      | [Notebook](./notebooks/cd35/annuaire_social.ipynb)           |
| cd62            | wikisol 62               | 24/05/2022 | [Notebook](./notebooks/cd62/analyse-cd62.ipynb)              |
| odspep          | ressources partenariales | 14/06/2022 | [Notebook](./notebooks/odspep/analyse.ipynb)                 |
| siao            | base siao                | 26/07/2022 | [Notebook](./notebooks/siao/analyse.ipynb)                   |
| soliguide       | lieux et services        | (api)      | [Notebook](./notebooks/soliguide/analyse.ipynb)              |
| Mes Aides       | garages solidaires       | (api)      | [Notebook](./notebooks/garages_solidaires/analyse-gs.ipynb)  |
| FINESS          | finess                   | (api)      | [Notebook](./notebooks/finess/analyse.ipynb)                 |
| Etab. Publics   | etablissements publics   | (api)      | [Notebook](./notebooks/etablissements-publics/Analyse.ipynb) |
| cd93            | organismes de formation  | 01/10/2022 | [Notebook](./notebooks/cd93/analyse.ipynb)                   |

## Contribuer

```bash
# Create a new virtualenv in the project's root directory
python3 -m venv .venv --prompt analyse

# Activate the environment
source .venv/bin/activate
pip install -U pip setuptools wheel

# Install dependencies
pip install -r requirements.txt

# Setup hook to clean notebook outputs
git config --local include.path ../analyse/.gitconfig
```
