# dc_leaderboard

Generateur de site statique de leaderboard pour les Data Challenges ocean et climat.

Le package produit un site HTML/CSS/JS avec:
- une page leaderboard (classement par metrique/variable/reference),
- une page maps (cartes interactives par bins lat/lon),
- une page about.

## Prerequis

- Python >= 3.11 et < 3.14
- Poetry

## Installation

```bash
# 1) Cloner le depot
# Adaptez l'URL a votre remote si besoin
git clone <URL_DU_DEPOT>
cd dc_leaderboard

# 2) Installer les dependances
poetry install
```

## Demarrage rapide (5 minutes)

Si vous avez deja un dossier `results/` avec des fichiers JSON:

```bash
# Generer le site statique
poetry run dcleaderboard-build \
  --results-dir dcleaderboard/results \
  --output-dir dcleaderboard/_site

# Ouvrir le leaderboard dans le navigateur
open dcleaderboard/_site/leaderboard.html
```

C'est tout! Vous avez maintenant:
- 📊 **leaderboard.html** - Classement des modeles par metrique
- 🗺️ **maps.html** - Cartes interactives (si fichiers per-bins presentes)
- ℹ️ **about.html** - Page informations

### Configuration basique (optionnel)

Pour personnaliser titres et labels, creez `dcleaderboard/results/leaderboard_config.yaml`:

```yaml
texts:
  page_title: "Mon Data Challenge"
  brand_name: "Ocean"
  brand_sub: "AI"

metrics_names:
  rmse: "Erreur Quadratique"
  bias: "Biais"

variables_names:
  ssh: "Hauteur de Surface"
  sst: "Temperature de Surface"
```

Puis regenerez:

```bash
poetry run dcleaderboard-build \
  --results-dir dcleaderboard/results \
  --output-dir dcleaderboard/_site \
  --config dcleaderboard/results/leaderboard_config.yaml
```

---

## Manuel d'utilisation detaille

### 1) Structure des donnees attendues

Le generateur s'attend a un dossier contenant des fichiers JSON avec les resultats de modeles.

**Format du fichier `results_*.json`:**

```json
{
  "metadata": {
    "model_name": "GloNet v1",
    "dataset_name": "ARGO",
    "version": "2024-01"
  },
  "results": [
    {
      "variable": "ssh",
      "reference": "argo",
      "metric": "rmse",
      "value": 0.0345
    },
    {
      "variable": "sst",
      "reference": "argo",
      "metric": "rmse",
      "value": 0.0123
    }
  ]
}
```

**Fichiers optionnels pour les cartes (per-bins):**

Si vous avez des resultats spatiaux par bins latitude/longitude:
- `*_per_bins.jsonl.gz` (recommande, compress)
- `*_per_bins.jsonl` (decompresse)
- `*_per_bins.json` (ancien format)

Ces fichiers doivent se trouver dans le meme dossier que les `results_*.json`.

**Exemple de structure:**

```text
mon_dossier_resultats/
  results_glonet.json
  results_glonet_per_bins.jsonl.gz
  results_baseline.json
  leaderboard_config.yaml        # (optionnel)
```

### 2) Options de generation

**Commande de base:**

```bash
poetry run dcleaderboard-build \
  --results-dir dcleaderboard/results \
  --output-dir dcleaderboard/_site
```

**Options CLI:**

| Option | Requis | Description |
|---|---|---|
| `--results-dir` | ✅ | Dossier contenant les fichiers `results_*.json` |
| `--output-dir` | ✅ | Dossier de sortie pour le site genere |
| `--template-dir` | | Dossier avec un `styles.css` personnalise |
| `--config` | | Chemin vers fichier YAML/JSON de configuration |
| `--site-base-url` | | URL de base (defaut: vide = chemins relatifs) |
| `--precision` | | Precision decimale cartes (defaut: 6) |
| `--skip-frt-snapshots` | | Optes snapshots temporels pour reduire taille |

**Exemples d'utilisation:**

```bash
# 1) Build simple avec config auto-detectable
poetry run dcleaderboard-build \
  --results-dir ./mes_resultats \
  --output-dir ./site_genere

# 2) Build avec precision reduite pour cartes (plus petit, moins precis)
poetry run dcleaderboard-build \
  --results-dir ./mes_resultats \
  --output-dir ./site_genere \
  --precision 2 \
  --skip-frt-snapshots

# 3) Build avec config et styles personnalises
poetry run dcleaderboard-build \
  --results-dir ./mes_resultats \
  --output-dir ./site_genere \
  --config ./ma_config.yaml \
  --template-dir ./mes_styles

# 4) Build pour deployer sur serveur
poetry run dcleaderboard-build \
  --results-dir ./mes_resultats \
  --output-dir ./site_production \
  --site-base-url "https://example.com/leaderboard"
```

**Auto-detection de config:**

Si `--config` n'est pas fourni, le build cherche automatiquement (dans l'ordre):
1. `leaderboard_config.yaml` dans `--results-dir`
2. `leaderboard_config.yml` dans `--results-dir`
3. `leaderboard_config.json` dans `--results-dir`
4. Les memes fichiers dans le dossier parent

Le premier fichier trouve est utilise.

### 3) Build local rapide (pour developpement)

Utile pour tester rapidement lors du developpement. Utilise les donnees du projet par defaut.

```bash
# Build dans dcleaderboard/_site/
poetry run python dcleaderboard/run_local.py

# Build avec config personnalisee
poetry run python dcleaderboard/run_local.py --config ./ma_config.yaml
```

Ensuite, ouvrez `dcleaderboard/_site/leaderboard.html` dans le navigateur.

### 4) Utiliser comme bibliotheque Python (integration dans vos scripts)

Pour integrer dcleaderboard dans vos propres projets:

```python
from pathlib import Path
import dcleaderboard

# A) Generer un site depuis un dossier de resultats
result = dcleaderboard.render_site_from_results_dir(
    results_dir="./mes_donnees",
    output_site_dir="./site_genere",
)
print(f"Site genere at: {result.leaderboard_html}")

# B) Generer depuis une liste de fichiers
result = dcleaderboard.render_site_from_results(
    results_files=[
        Path("./mes_donnees/results_model1.json"),
        Path("./mes_donnees/results_model2.json"),
    ],
    output_site_dir="./site_genere",
    precision=8,  # Haute precision pour cartes
)

# C) Charger et explorer les donnees
df = dcleaderboard.load_data("./mes_donnees")
print(df.head())

# D) Generer les rapports
for item_type, content in dcleaderboard.generate_report_items(df):
    print(f"Type: {item_type}")
    if item_type == "markdown":
        print(content[:100])
```

---

## Configuration

Personnalisez l'apparence et les labels du leaderboard via un fichier `leaderboard_config.yaml`.

### Fichiers de reference

- [dcleaderboard/results/leaderboard_config.yaml](dcleaderboard/results/leaderboard_config.yaml) - Exemple complet avec toutes les options
- [dcleaderboard/config/leaderboard_texts.yaml](dcleaderboard/config/leaderboard_texts.yaml) - Configuration par defaut minimale

### Exemple minimal

Creez `leaderboard_config.yaml` dans votre dossier de resultats:

```yaml
# ==== SECTION 1: Textes de la page ====
texts:
  page_title: "Mon Data Challenge 2024"
  brand_name: "Ocean & Climate"
  brand_sub: "Challenge Data"
  github_url: "https://github.com/mon-org/mon-repo"

# ==== SECTION 2: Renommer les metriques ====
metrics_names:
  rmse: "Erreur Quadratique"
  rmsd: "Deviation Quadratique"
  bias: "Biais Moyen"

# ==== SECTION 3: Renommer les variables ====
variables_names:
  ssh: "Hauteur Surface Mer"
  sst: "Temperature Surface"
  sss: "Salinite Surface"
  u: "Vitesse U"
  v: "Vitesse V"

# ==== SECTION 4: Renommer les modeles ====
models_names:
  glonet: "GloNet v1 (Reference)"
  model_baseline: "Baseline Simple"
  model_ml: "ML Challenger"
```

Le build detectera automatiquement ce fichier. Vous pouvez aussi le passer explicitement:

```bash
poetry run dcleaderboard-build \
  --results-dir ./mes_resultats \
  --output-dir ./site_en_ligne \
  --config ./mes_resultats/leaderboard_config.yaml
```

### Qu'est-ce qu'on peut personnaliser?

- **Textes** (titre page, brand, GitHub, templates de sections)
- **Labels des metriques** (RMSE → "Erreur Quadratique", etc.)
- **Labels des variables** (ssh → "Hauteur Surface", etc.)
- **Labels des modeles** (glonet → "GloNet Reference", etc.)
- **Styles CSS** (si fourni via `--template-dir`)

Voir [dcleaderboard/results/leaderboard_config.yaml](dcleaderboard/results/leaderboard_config.yaml) pour la liste complete des options.

---

## Structure du projet

```text
dc_leaderboard/
  pyproject.toml
  README.md
  dcleaderboard/
    __init__.py
    build.py
    processing.py
    html_builder.py
    map_processing.py
    map_builder.py
    run_local.py
    styles.css
    config/
    results/
  test_api.py
```

## Verification et developpement

### Tester le package

```bash
# Tests unitaires
poetry run pytest

# Verifier le format du code (linting)
poetry run ruff check .

# Verifier les types Python
poetry run mypy dcleaderboard

# Verifier rapidement l'API publique
poetry run python test_api.py
```

### Generer un site de test

```bash
# Utilise les donnees d'exemple du projet
poetry run python dcleaderboard/run_local.py

# Puis ouvrez: dcleaderboard/_site/leaderboard.html
open dcleaderboard/_site/leaderboard.html
```

---

## Guides de depannage

### Probleme: "Module dcleaderboard not found"

**Cause:** Le package n'est pas installe.

**Solution:**
```bash
poetry install
poetry run dcleaderboard-build --help
```

### Probleme: "No results files provided"

**Cause:** Aucun fichier `results_*.json` trouvé dans le dossier.

**Solution:**
- Verifiez le chemin: `ls mes_resultats/results_*.json`
- Assurez-vous que les fichiers sont nommes `results_XXXX.json`

### Probleme: Leaderboard vide (pas de donnees)

**Cause:** Format JSON incorrect.

**Solution:**
- Verifiez un fichier `results_*.json` – doit avoir structure:
  ```json
  {
    "metadata": {...},
    "results": [{"variable": "ssh", "metric": "rmse", "value": 0.123}, ...]
  }
  ```
- Testez avec: `cat mes_resultats/results_*.json | python -m json.tool`

### Probleme: Cartes interactives manquantes

**Cause:** Fichiers `*_per_bins.*` absent ou format incorrect.

**Solution:**
- Verifiez: `ls mes_resultats/*_per_bins*`
- Formats acceptes: `.jsonl.gz`, `.jsonl`, `.json`
- Les fichiers doivent etre dans le meme dossier que `results_*.json`

### Probleme: La config personnalisee n'est pas appliquee

**Cause:** Le fichier YAML est mal positionne.

**Solution:**
- Config auto-detectable – doit etre dans `--results-dir` ou son parent
- Ou fourni explicitement: `--config path/to/config.yaml`
- Verifiez la syntaxe YAML: `cat config.yaml | python -m yaml`

### Probleme: CSS personnalise ne s'applique pas

**Cause:** Le chemin `--template-dir` est incorrect.

**Solution:**
```bash
# Verifiez que le dossier contient styles.css
ls mon_dossier_styles/styles.css

# Puis regenerez
poetry run dcleaderboard-build \
  --results-dir mes_resultats \
  --output-dir site_output \
  --template-dir mon_dossier_styles
```

---

## Besoin d'aide?

1. **Verifiez les logs:** Regardez les sorties en ligne de commande (messages d'erreur)
2. **Testez l'API:** `poetry run python test_api.py`
3. **Consultez les exemples:** `dcleaderboard/results/leaderboard_config.yaml`
4. **Executez le build local:** `poetry run python dcleaderboard/run_local.py`
