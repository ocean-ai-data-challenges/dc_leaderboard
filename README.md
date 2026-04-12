# dc_leaderboard

Générateur de sites web de leaderboard pour les Data Challenges océan & climat.

Produit un site statique (HTML/CSS/JS) contenant :
- un **leaderboard** comparant les modèles sur différentes métriques et variables,
- des **cartes interactives** de performances spatiales (par bins lat/lon),
- une page **About**.

## Installation

Le projet utilise **Poetry** pour la gestion des dépendances.

```bash
# Cloner le dépôt
git clone https://github.com/ppr-ocean-ia/dc-tools.git
cd dc_leaderboard

# Installer les dépendances (et le package en mode éditable)
poetry install
```

> **Python requis :** ≥ 3.11, < 3.14

## Utilisation

### En ligne de commande (CLI)

```bash
dcleaderboard-build --results-dir path/to/results --output-dir path/to/output
```

Options :

| Option | Description |
|---|---|
| `--results-dir` | **(obligatoire)** Répertoire contenant les fichiers de résultats JSON |
| `--output-dir` | **(obligatoire)** Répertoire de sortie pour le site généré |
| `--config` | Fichier de configuration YAML/JSON (auto-détecté sinon) |
| `--template-dir` | Répertoire contenant un `styles.css` personnalisé |
| `--site-base-url` | URL de base du site (par défaut : chemins relatifs) |

Le CLI détecte automatiquement un fichier `leaderboard_config.yaml` (ou `.yml` / `.json`) dans le répertoire de résultats ou son parent.

### En tant que bibliothèque Python

```python
import dcleaderboard

# Générer un site complet à partir d'un répertoire de résultats
dcleaderboard.render_site_from_results_dir(
    results_dir="path/to/results",
    output_dir="path/to/output",
)

# Charger les données et générer les items du rapport
df = dcleaderboard.load_data("path/to/results")
for kind, content in dcleaderboard.generate_report_items(df):
    print(kind, content)
```

### Développement local

```bash
# Génère le site dans dcleaderboard/_site/
python dcleaderboard/run_local.py [--config path/to/config.yaml]
```

Par défaut, les résultats sont lus depuis `dcleaderboard/results/` et le site est écrit dans `dcleaderboard/_site/`.

## Configuration

La configuration est un fichier YAML (ou JSON) optionnel permettant de personnaliser le rendu. Exemple :

```yaml
# Titre de la page
page_title: "GloNet Data Challenge Leaderboard"
brand_name: "Ocean & Climate"
brand_sub: "Data Challenge"

# Noms d'affichage des métriques
metrics_names:
  rmse: "Root Mean Squared Error"
  bias: "Bias"

# Noms d'affichage des variables
variables_names:
  ssh: "Sea Surface Height"
  sst: "Sea Surface Temperature"

# Noms d'affichage des modèles
models_names:
  glonet: "GloNet v1"

# Filtrage des métriques affichées
allowed_metrics:
  - rmse
  - bias
```

Voir `dcleaderboard/results/leaderboard_config.yaml` pour un exemple complet.

## Structure du projet

```
dc_leaderboard/
├── pyproject.toml              # Métadonnées & dépendances (Poetry)
├── README.md
├── dcleaderboard/
│   ├── __init__.py             # API publique
│   ├── build.py                # CLI et fonctions de génération du site
│   ├── processing.py           # Chargement des données et génération du rapport
│   ├── html_builder.py         # Construction des pages HTML
│   ├── map_processing.py       # Traitement des données spatiales (per-bins)
│   ├── map_builder.py          # Construction de la page de cartes
│   ├── styles.css              # Styles du site
│   ├── run_local.py            # Script de développement local
│   ├── config/                 # Configuration par défaut
│   └── results/                # Données de résultats d'exemple
│       ├── leaderboard_config.yaml
│       └── *.json
└── tests/
```

## Développement

```bash
# Tests
poetry run pytest

# Linting
poetry run ruff check .

# Type checking
poetry run mypy dcleaderboard
```
