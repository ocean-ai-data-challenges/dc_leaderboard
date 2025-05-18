# Variables
PYTHON=python3
QUARTO=quarto
INPUT_QMD=leaderboard.qmd
OUTPUT_HTML=leaderboard.html

.PHONY: all clean html

all: html

html:
	@echo "🛠️ Compilation Quarto..."
	$(QUARTO) render $(INPUT_QMD) --to html

clean:
	@echo "🧹 Nettoyage..."
	rm -f method_data.js $(OUTPUT_HTML)
