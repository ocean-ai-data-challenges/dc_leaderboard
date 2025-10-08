# Configuration Conda pour DC Leaderboard

Ce projet utilise maintenant une configuration 100% conda pour gérer les dépendances.

## Installation

### 1. Créer l'environnement conda

```bash
# Depuis la racine du projet
cd /home/k24aitmo/IMT/software/dc_leaderboard/dcleaderboard
make -f Makefile.conda install
```

### 2. Activer l'environnement

```bash
# Option 1: Avec le script d'activation
source ../activate_env.sh

# Option 2: Directement avec conda
conda activate dcleaderboard
```

### 3. Compiler le leaderboard

```bash
# Avec l'environnement activé
make -f Makefile.conda all

# Ou en une seule commande (sans activation préalable)
make -f Makefile.conda all
```

## Commandes utiles

```bash
# Créer/mettre à jour l'environnement
make -f Makefile.conda install

# Compiler le leaderboard
make -f Makefile.conda all

# Tester l'environnement
make -f Makefile.conda test-env

# Nettoyer les fichiers générés
make -f Makefile.conda clean

# Supprimer complètement l'environnement
make -f Makefile.conda remove-env

# Afficher l'aide
make -f Makefile.conda help
```

## Structure des fichiers

- `environment.yml` : Configuration des dépendances conda
- `Makefile.conda` : Makefile adapté pour conda
- `activate_env.sh` : Script d'activation de l'environnement
- `README-conda.md` : Cette documentation

## Migration depuis Poetry

Si vous utilisiez Poetry auparavant :

1. Désactivez l'environnement Poetry : `conda deactivate` ou `exit` du shell Poetry
2. Supprimez l'environnement virtuel Poetry si souhaité : `poetry env remove python`
3. Suivez les étapes d'installation ci-dessus

## Dépendances

Toutes les dépendances du `pyproject.toml` ont été migrées vers `environment.yml` :

- Python 3.11+
- Pandas, NumPy, Matplotlib
- Jupyter, JupyterLab, IPython
- Quarto (via pip)
- Outils de développement (pytest, ruff, mypy)
- Outils de documentation (sphinx, myst-parser)

## Résolution des problèmes

### Quarto non trouvé
```bash
# Réinstaller quarto dans l'environnement
conda activate dcleaderboard
pip install quarto
```

### Erreurs de kernel Jupyter
```bash
# Réenregistrer le kernel
conda activate dcleaderboard
python -m ipykernel install --user --name dcleaderboard
```

### Conflits de dépendances
```bash
# Mettre à jour l'environnement
make -f Makefile.conda update-env
```
