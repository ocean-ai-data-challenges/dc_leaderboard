#!/usr/bin/env python3
"""
Script pour générer le leaderboard sans Quarto
Extrait le code Python du fichier QMD et l'exécute directement
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path
import re
from IPython.display import display, Markdown

def extract_python_code_from_qmd(qmd_file):
    """Extrait le code Python d'un fichier QMD"""
    with open(qmd_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Extraire les blocs de code Python
    python_blocks = re.findall(r'```\{python\}(.*?)```', content, re.DOTALL)
    
    return '\n'.join(python_blocks)

def main():
    print("🔧 Génération du leaderboard sans Quarto...")
    
    # Extraire et exécuter le code Python
    python_code = extract_python_code_from_qmd('leaderboard.qmd')
    
    print("📝 Code Python extrait:")
    print("=" * 50)
    print(python_code[:500] + "..." if len(python_code) > 500 else python_code)
    print("=" * 50)
    
    # Exécuter le code dans l'environnement local
    local_vars = {}
    global_vars = {
        '__name__': '__main__',
        'pd': pd,
        'np': np,
        'json': json,
        'Path': Path,
        're': re,
        'display': print,  # Remplacer display par print
        'Markdown': lambda x: print(x),  # Remplacer Markdown par print
    }
    
    try:
        exec(python_code, global_vars, local_vars)
        print("✅ Code exécuté avec succès!")
        
        # Sauvegarder les résultats si un DataFrame a été créé
        if 'df' in local_vars and isinstance(local_vars['df'], pd.DataFrame):
            df = local_vars['df']
            print(f"📊 DataFrame créé avec {len(df)} lignes et {len(df.columns)} colonnes")
            
            # Générer un HTML simple
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Data Challenge 2 Leaderboard</title>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        h1 {{ color: #333; text-align: center; }}
        table {{ margin: 20px auto; border-collapse: collapse; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: center; }}
        th {{ background-color: #f2f2f2; }}
        .date {{ text-align: center; color: #666; margin-top: 20px; }}
    </style>
</head>
<body>
    <h1>Data Challenge 2 Leaderboard</h1>
    <h2>Probabilistic short-term forecasting of global ocean dynamics</h2>
    
    {df.to_html(classes='table', table_id='leaderboard')}
    
    <div class="date">Généré le {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
</body>
</html>
"""
            
            with open('leaderboard.html', 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            print("✅ Fichier leaderboard.html généré!")
            
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
