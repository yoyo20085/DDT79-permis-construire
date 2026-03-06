# packages.R
# Installation de tous les packages nécessaires au projet.
# Exécuté une seule fois par GitHub Actions (résultat mis en cache).
# ─────────────────────────────────────────────────────────────

options(repos = c(CRAN = "https://cloud.r-project.org"))

pkgs <- c(
  "httr",          # téléchargement API DiDo
  "readr",         # lecture CSV grande taille
  "dplyr",         # manipulation données
  "lubridate",     # gestion des dates
  "sf",            # géométries GeoJSON
  "flexdashboard", # structure du dashboard
  "plotly",        # graphiques interactifs
  "leaflet",       # carte choroplèthe
  "rmarkdown",     # génération HTML
  "knitr"          # moteur Rmd
)

cat("Installation des packages...\n")
install.packages(pkgs, dependencies = TRUE)
cat("✅ Packages installés :", paste(pkgs, collapse = ", "), "\n")
