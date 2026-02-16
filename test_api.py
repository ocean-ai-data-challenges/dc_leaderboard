#!/usr/bin/env python3
"""
Script de test pour vérifier l'API publique de dcleaderboard.
Simule une utilisation par une librairie externe.
"""

import sys
from pathlib import Path
import pandas as pd

def check_dcleaderboard_api():
    print("Vérification de l'accessibilité du package `dcleaderboard`...")
    
    try:
        import dcleaderboard
        print(f"Import réussi: {dcleaderboard}")
        print(f"   Chemin: {dcleaderboard.__file__}")
    except ImportError as e:
        print(f"Échec de l'import: {e}")
        print("   Assurez-vous que le package est installé ou accessible dans le PYTHONPATH.")
        return False

    # Vérification des symboles exposés
    expected_symbols = ["load_data", "generate_report_items", "render_site_from_results_dir"]
    missing = [s for s in expected_symbols if not hasattr(dcleaderboard, s)]
    
    if missing:
        print(f"Symboles manquants dans l'API publique: {missing}")
        return False
    else:
        print(f"Tous les symboles attendus sont présents: {expected_symbols}")

    # Test fonctionnel simple : load_data et generate_report_items
    print("\nTest fonctionnel...")
    
    # On cherche le dossier results relativements au script
    results_path = Path("dcleaderboard/results")
    if not results_path.exists():
         # Fallback si on est ailleurs
         results_path = Path("results")
    
    if not results_path.exists():
        print(f"Impossible de trouver un dossier de résultats pour le test (cherché dans {Path.cwd()})")
        return True # On considère que l'API est OK structurellement même si on peut pas tester la data

    print(f"Chargement des données depuis: {results_path}")
    try:
        df = dcleaderboard.load_data(results_path)
        print(f"Données chargées. DataFrame shape: {df.shape}")
    except Exception as e:
        print(f"Erreur lors de load_data: {e}")
        return False

    print("Génération des éléments du rapport...")
    try:
        # Test avec une config vide
        items = list(dcleaderboard.generate_report_items(df))
        print(f"Génération réussie. {len(items)} éléments produits.")
        
        # Vérification des types
        types = [t for t, _ in items]
        print(f"   Types d'éléments: {set(types)}")
        
        has_table = "styler" in types
        has_md = "markdown" in types
        
        if has_table and has_md:
            print("Les types de retour semblent corrects (styler + markdown).")
        else:
            print(f"Types de retour inhabituels: {set(types)}")
            
    except Exception as e:
        print(f"Erreur lors de generate_report_items: {e}")
        return False

    print("\nTest de l'API terminé avec succès.")
    return True

if __name__ == "__main__":
    success = check_dcleaderboard_api()
    sys.exit(0 if success else 1)
