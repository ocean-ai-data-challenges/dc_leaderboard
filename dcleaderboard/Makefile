# Variables
QUARTO=poetry run quarto
INPUT_QMD=leaderboard.qmd
ABOUT_QMD=about.qmd
OUTPUT_HTML=leaderboard.html
ABOUT_HTML=about.html

.PHONY: all clean html

all: html

html:
	@echo "🛠️ Compilation Quarto..."
	$(QUARTO) render $(INPUT_QMD) --to html
	$(QUARTO) render $(ABOUT_QMD) --to html

clean:
	@echo "🧹 Nettoyage..."
	rm -f $(OUTPUT_HTML) $(ABOUT_HTML)
