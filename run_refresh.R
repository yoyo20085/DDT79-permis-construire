# run_refresh.R
# ─────────────────────────────────────────────────────────────
# Script principal exécuté par GitHub Actions chaque mois.
# Orchestre :
#   1. Téléchargement des données Sitadel via l'API DiDo
#   2. Génération du dashboard HTML statique
#   3. Dépôt dans docs/index.html (servi par GitHub Pages)
#
# Peut aussi être lancé manuellement en local :
#   Rscript run_refresh.R
# ─────────────────────────────────────────────────────────────

cat("╔══════════════════════════════════════════════════════╗\n")
cat("║   Refresh Sitadel 79 —", format(Sys.time()), "   ║\n")
cat("╚══════════════════════════════════════════════════════╝\n\n")

# ── Options ───────────────────────────────────────────────────
# Sur GitHub Actions, FORCE_REFRESH est défini dans le workflow.
# En local, on force toujours si le cache a plus de 30 jours.
force_refresh <- isTRUE(as.logical(Sys.getenv("FORCE_REFRESH", "false")))

options(repos = c(CRAN = "https://cloud.r-project.org"))

# ── 1. Chargement des données Sitadel ────────────────────────
cat("── ÉTAPE 1 : Téléchargement des données ────────────────\n")

source("01_fetch_sitadel_79.R")   # charge les fonctions ETL

donnees <- charger_donnees_sitadel(forcer_refresh = force_refresh)

if (is.null(donnees$logements) || nrow(donnees$logements) == 0) {
  stop("❌ Données logements vides — arrêt du script.")
}
cat("✅ Données chargées :",
    nrow(donnees$logements), "logements |",
    nrow(donnees$locaux), "locaux\n\n")

# ── 2. Génération du dashboard HTML ──────────────────────────
cat("── ÉTAPE 2 : Génération du dashboard HTML ───────────────\n")

# Créer le dossier docs/ s'il n'existe pas (servi par GitHub Pages)
if (!dir.exists("docs")) dir.create("docs")

rmarkdown::render(
  input       = "DD79_appli_github.Rmd",
  output_file = "index.html",
  output_dir  = "docs",
  quiet       = FALSE,
  envir       = new.env(parent = globalenv())
)

# ── 3. Vérification ──────────────────────────────────────────
html_path <- file.path("docs", "index.html")

if (file.exists(html_path)) {
  taille <- round(file.size(html_path) / 1024^2, 1)
  cat("\n✅ Dashboard généré :", html_path, "(", taille, "Mo)\n")
  cat("   Mise à jour :", format(Sys.time(), "%d/%m/%Y à %H:%M"), "\n")
} else {
  stop("❌ Le fichier docs/index.html n'a pas été généré.")
}

cat("\n╔══════════════════════════════════════════════════════╗\n")
cat("║   ✅ Refresh terminé avec succès                     ║\n")
cat("╚══════════════════════════════════════════════════════╝\n")
