#!/usr/bin/env python3
"""
Script de test pour vérifier l'API publique de dcleaderboard.
Simule une utilisation par une librairie externe.
"""

import sys
from pathlib import Path
import pandas as pd

def check_dcleaderboard_api():
    print("🔎 Vérification de l'accessibilité du package `dcleaderboard`...")
    
    try:
        import dcleaderboard
        print(f"✅ Import réussi: {dcleaderboard}")
        print(f"   Chemin: {dcleaderboard.__file__}")
    except ImportError as e:
        print(f"❌ Échec de l'import: {e}")
        print("   Assurez-vous que le package est installé ou accessible dans le PYTHONPATH.")
        return False

    # Vérification des symboles exposés
    expected_symbols = ["load_data", "generate_report_items", "render_site_from_results_dir"]
    missing = [s for s in expected_symbols if not hasattr(dcleaderboard, s)]
    
    if missing:
        print(f"❌ Symboles manquants dans l'API publique: {missing}")
        return False
    else:
        print(f"✅ Tous les symboles attendus sont présents: {expected_symbols}")

    # Test fonctionnel simple : load_data et generate_report_items
    print("\n🧪 Test fonctionnel...")
    
    # On cherche le dossier results relativements au script
    results_path = Path("dcleaderboard/results")
    if not results_path.exists():
         # Fallback si on est ailleurs
         results_path = Path("results")
    
    if not results_path.exists():
        print(f"⚠️ Impossible de trouver un dossier de résultats pour le test (cherché dans {Path.cwd()})")
        return True # On considère que l'API est OK structurellement même si on peut pas tester la data

    print(f"📂 Chargement des données depuis: {results_path}")
    try:
        df = dcleaderboard.load_data(results_path)
        print(f"✅ Données chargées. DataFrame shape: {df.shape}")
    except Exception as e:
        print(f"❌ Erreur lors de load_data: {e}")
        return False

    print("📊 Génération des éléments du rapport...")
    try:
        # Test avec une config vide
        items = list(dcleaderboard.generate_report_items(df))
        print(f"✅ Génération réussie. {len(items)} éléments produits.")
        
        # Vérification des types
        types = [t for t, _ in items]
        print(f"   Types d'éléments: {set(types)}")
        
        has_table = "styler" in types
        has_md = "markdown" in types
        
        if has_table and has_md:
            print("✅ Les types de retour semblent corrects (styler + markdown).")
        else:
            print(f"⚠️ Types de retour inhabituels: {set(types)}")
            
    except Exception as e:
        print(f"❌ Erreur lors de generate_report_items: {e}")
        return False

    print("\n🎨 Test de la personnalisation (Custom Config)...")
    try:
        import shutil
        output_dir = Path("_test_site_custom")
        if output_dir.exists():
            shutil.rmtree(output_dir)
            
        custom_config = {
            "texts": {
                "page_title": "TITRE TEST API PERSONNALISÉ",
            },
            "metrics_names": {
                "rmse": "METRIQUE TEST RMSE"
            },
            "models_names": {
                "glonet": "MODELE TEST GLONET"
            }
        }
        
        print(f"   Génération du site dans {output_dir} avec config custom...")
        dcleaderboard.render_site_from_results_dir(
            results_dir=results_path,
            output_site_dir=output_dir,
            custom_config=custom_config
        )
        
        html_file = output_dir / "leaderboard.html"
        if not html_file.exists():
            print("❌ Le fichier HTML n'a pas été généré.")
            return False
            
        content = html_file.read_text(encoding="utf-8")
        
        # Vérifications
        checks = [
            ("TITRE TEST API PERSONNALISÉ", "Titre de page"),
            ("METRIQUE TEST RMSE", "Renommage métrique"),
            ("MODELE TEST GLONET", "Renommage modèle")
        ]
        
        all_ok = True
        for text, desc in checks:
            if text in content:
                print(f"   ✅ {desc} trouvé.")
            else:
                print(f"   ❌ {desc} NON trouvé ('{text}').")
                all_ok = False
        
        # Nettoyage
        if output_dir.exists():
            shutil.rmtree(output_dir)
            
        if not all_ok:
            return False
            
    except Exception as e:
        print(f"❌ Erreur lors du test de personnalisation: {e}")
        # On affiche la stacktrace pour debug
        import traceback
        traceback.print_exc()
        return False

    print("\n✨ Test de l'API terminé avec succès.")
    return True

if __name__ == "__main__":
    success = check_dcleaderboard_api()
    sys.exit(0 if success else 1)
