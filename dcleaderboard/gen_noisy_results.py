import json
import sys
import numpy as np

def add_noise(obj, std_rel):
    if isinstance(obj, dict):
        return {k: add_noise(v, std_rel) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [add_noise(v, std_rel) for v in obj]
    elif isinstance(obj, (int, float)):
        noise = np.random.normal(0, abs(obj) * std_rel)
        return float(round(obj + noise, 8))
    else:
        return obj

def process_file(src_path, dst_path, new_model, std_rel=0.05):
    with open(src_path) as f:
        data = json.load(f)
    # Remplace dataset
    data["dataset"] = new_model
    # Remplace model dans toutes les entrées
    if "results" in data and isinstance(data["results"], dict):
        # On récupère toutes les entrées, peu importe l'ancien nom du modèle
        all_entries = []
        for entries in data["results"].values():
            all_entries.extend(entries)
        # On modifie le champ "model" dans chaque entrée
        for entry in all_entries:
            entry["model"] = new_model
            if "result" in entry:
                entry["result"] = add_noise(entry["result"], std_rel)
        # On remplace la clé dans "results" par le nouveau nom
        data["results"] = {new_model: all_entries}
    with open(dst_path, "w") as f:
        json.dump(data, f, indent=2)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python gen_noisy_results.py <src.json> <dst.json> <new_model> [std_rel]")
        sys.exit(1)
    src, dst, new_model = sys.argv[1:4]
    std_rel = float(sys.argv[4]) if len(sys.argv) > 4 else 0.05
    np.random.seed(42)
    process_file(src, dst, new_model, std_rel)
    
    
    # python gen_noisy_results.py results/results_glonet.json results/results_challenger_model_1.json challenger_model_1 0.08
