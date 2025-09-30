import json
import numpy as np

# Paramètres du bruit gaussien (modifiable)
NOISE_STD_REL = 0.09  # 5% d'écart-type relatif (modifiable)

def add_noise(value, std_rel=NOISE_STD_REL):
    """Ajoute un bruit gaussien relatif à la valeur."""
    if isinstance(value, (int, float)):
        noise = np.random.normal(0, abs(value) * std_rel)
        return float(round(value + noise, 6))
    return value

def process_result(result, std_rel=NOISE_STD_REL):
    """Ajoute du bruit aux valeurs numériques dans le bloc result."""
    if result is None:
        return None
    if isinstance(result, list):
        new_list = []
        for item in result:
            new_item = {}
            for k, v in item.items():
                if k == "global" and isinstance(v, dict):
                    new_item[k] = {kk: add_noise(vv, std_rel) for kk, vv in v.items()}
                elif k == "per_bins":
                    new_item[k] = v  # on ne touche pas aux per_bins
                else:
                    new_item[k] = v
            new_list.append(new_item)
        return new_list
    if isinstance(result, dict):
        new_dict = {}
        for metric, variables in result.items():
            new_dict[metric] = {var: add_noise(val, std_rel) for var, val in variables.items()}
        return new_dict
    return result

def main():
    np.random.seed(42)  # Pour reproductibilité
    input_file = "results/glonet.json"
    output_file = "results/glonet_noisy.json"

    with open(input_file, "r") as f:
        data = json.load(f)

    new_data = {"glonet": []}
    for entry in data["glonet"]:
        new_entry = entry.copy()
        new_entry["result"] = process_result(entry.get("result"), std_rel=NOISE_STD_REL)
        new_data["glonet"].append(new_entry)

    with open(output_file, "w") as f:
        json.dump(new_data, f, indent=2)

    print(f"Fichier généré : {output_file} (bruit gaussien σ={NOISE_STD_REL*100:.1f}%)")

if __name__ == "__main__":
    main()
