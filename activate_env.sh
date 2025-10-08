#!/bin/bash
# Script pour activer l'environnement conda dcleaderboard

echo "🔧 Activation de l'environnement conda dcleaderboard..."

# Vérifier que conda est disponible
if ! command -v conda &> /dev/null; then
    echo "❌ Conda n'est pas installé ou pas dans le PATH"
    exit 1
fi

# Initialiser conda pour bash si nécessaire
if [ -z "$CONDA_DEFAULT_ENV" ]; then
    source $(conda info --base)/etc/profile.d/conda.sh
fi

# Vérifier que l'environnement existe
if ! conda env list | grep -q "^dcleaderboard "; then
    echo "❌ L'environnement 'dcleaderboard' n'existe pas."
    echo "📦 Créez-le avec: make -f Makefile.conda install"
    exit 1
fi

# Activer l'environnement
conda activate dcleaderboard

echo "✅ Environnement dcleaderboard activé!"
echo "🔧 Pour compiler le leaderboard: make -f Makefile.conda all"
echo "🔧 Pour désactiver: conda deactivate"
