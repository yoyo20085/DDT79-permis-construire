# ============================================================
# 01_fetch_sitadel_79.R
# Récupération des données Sitadel via l'API DiDo — DEP 79
#
# Conclusion des tests :
#   L'API DiDo N'APPLIQUE PAS les filtres géographiques
#   côté serveur sur l'export CSV — elle envoie toujours
#   le fichier national complet (~200 Mo par fichier).
#   → On télécharge le fichier complet en chunks et on
#     filtre localement sur DEP_CODE = "79".
#
# Temps estimé premier lancement : 10-20 min (2 fichiers ~200Mo)
# Lancements suivants (cache) : < 1 seconde
# ============================================================

library(httr)
library(readr)
library(dplyr)
library(lubridate)
library(sf)

`%||%` <- function(a, b) if (!is.null(a)) a else b

# ── Configuration ─────────────────────────────────────────────
API_BASE      <- "https://data.statistiques.developpement-durable.gouv.fr/dido/api/v1"
DEP_CIBLE     <- "79"
CACHE_DIR     <- "data_cache"

# RIDs confirmés par diagnostic
RID_LOGEMENTS <- "8b35affb-55fc-4c1f-915b-7750f974446a"
RID_LOCAUX    <- "f8f0700f-806c-40a7-83b1-f21cf507e7c4"

if (!dir.exists(CACHE_DIR)) dir.create(CACHE_DIR)
message("=== Démarrage récupération Sitadel 79 — ", Sys.time(), " ===")


# ════════════════════════════════════════════════════════════
# 1. Dernier millésime disponible
# ════════════════════════════════════════════════════════════
get_dernier_millesime <- function(rid) {
  url  <- paste0(API_BASE, "/datafiles/", rid)
  resp <- GET(url, timeout(30))
  stop_for_status(resp)
  meta <- content(resp, as = "parsed", encoding = "UTF-8")

  if (!is.null(meta$millesimes) && length(meta$millesimes) > 0) {
    m <- meta$millesimes[[1]]$millesime
    n <- meta$millesimes[[1]]$rows
    message("  Millésime : ", m,
            " | ", format(n, big.mark = " "), " lignes nationales")
    return(m)
  }
  return(meta$millesime %||% NULL)
}


# ════════════════════════════════════════════════════════════
# 2. Téléchargement du fichier CSV complet + filtrage local
#
#    Le fichier est téléchargé dans un fichier temporaire
#    puis lu avec readr en filtrant sur DEP_CODE = "79".
#    Timeout à 30 minutes pour absorber les ~200 Mo.
# ════════════════════════════════════════════════════════════
fetch_et_filtrer_dep79 <- function(rid, millesime, label = "") {

  url <- paste0(
    API_BASE, "/datafiles/", rid, "/csv",
    "?millesime=", millesime,
    "&withColumnName=true",
    "&withColumnDescription=false",
    "&withColumnUnit=false"
  )

  tmp <- tempfile(fileext = ".csv")
  message("  ⬇️  Téléchargement ", label, " (fichier national, ~200 Mo) ...")
  message("      Veuillez patienter — peut prendre 5 à 15 min...")

  debut <- Sys.time()

  resp <- tryCatch(
    GET(url,
        write_disk(tmp, overwrite = TRUE),
        timeout(1800),   # 30 minutes max
        progress()),
    error = function(e) {
      message("  ❌ Erreur réseau : ", e$message)
      return(NULL)
    }
  )

  if (is.null(resp) || http_error(resp)) {
    unlink(tmp)
    stop("Téléchargement échoué (HTTP ", status_code(resp), ")")
  }

  taille_mo <- round(file.size(tmp) / 1024^2, 1)
  duree_sec <- round(as.numeric(difftime(Sys.time(), debut, units = "secs")))
  message("  ✅ Téléchargé : ", taille_mo, " Mo en ", duree_sec, " sec")

  # Lecture en chunks pour économiser la RAM
  message("  🔍 Filtrage DEP_CODE = ", DEP_CIBLE, " ...")

  df <- tryCatch({
    # Lecture complète avec readr (gère bien les grands CSV)
    d <- read_delim(
      tmp,
      delim          = ";",
      col_types      = cols(
        DEP_CODE = col_character(),
        COMM     = col_character(),
        REG_CODE = col_character(),
        .default = col_guess()
      ),
      locale         = locale(encoding    = "UTF-8",
                              decimal_mark = ","),
      show_col_types = FALSE,
      progress       = FALSE
    )

    message("  Lignes nationales lues : ", format(nrow(d), big.mark = " "))

    # Filtrage sur DEP_CODE = "79"
    d_dep <- d %>% filter(DEP_CODE == DEP_CIBLE)
    message("  Lignes DEP=", DEP_CIBLE, " : ",
            format(nrow(d_dep), big.mark = " "),
            " (", n_distinct(d_dep$COMM), " communes)")

    rm(d)   # libérer la mémoire
    gc()
    d_dep

  }, error = function(e) {
    message("  ❌ Erreur lecture CSV : ", e$message)
    data.frame()
  })

  unlink(tmp)
  return(df)
}


# ════════════════════════════════════════════════════════════
# 3. Nettoyage logements
# ════════════════════════════════════════════════════════════
nettoyer_logements <- function(df) {

  # Dates
  for (col in intersect(names(df), c("DATE_REELLE_AUTORISATION",
                                      "DATE_REELLE_DOC",
                                      "DATE_REELLE_DAACT"))) {
    df[[col]] <- suppressWarnings(ymd(df[[col]]))
  }

  # Numériques
  for (col in intersect(names(df), c(
    "NB_LGT_TOT_CREES", "NB_LGT_IND_CREES", "NB_LGT_COL_CREES",
    "NB_LGT_INDIV_PURS", "NB_LGT_INDIV_GROUPES",
    "NB_LGT_COL_HORS_RES", "NB_LGT_RES", "NB_LGT_DEMOLIS",
    "NB_LGT_1P", "NB_LGT_2P", "NB_LGT_3P",
    "NB_LGT_4P", "NB_LGT_5P", "NB_LGT_6P_PLUS",
    "NB_LGT_PRET_LOC_SOCIAL", "NB_LGT_PTZ",
    "SURF_HAB_CREEE", "SURF_HAB_DEMOLIE", "SURF_LOC_CREEE"
  ))) df[[col]] <- suppressWarnings(as.numeric(df[[col]]))

  # Colonnes temporelles
  if ("DATE_REELLE_AUTORISATION" %in% names(df)) {
    df <- df %>% mutate(
      ANNEE      = year(DATE_REELLE_AUTORISATION),
      MOIS       = month(DATE_REELLE_AUTORISATION),
      TRIMESTRE  = quarter(DATE_REELLE_AUTORISATION),
      ANNEE_MOIS = format(DATE_REELLE_AUTORISATION, "%Y-%m"),
      ANNEE_TRIM = paste0(year(DATE_REELLE_AUTORISATION), "-T",
                          quarter(DATE_REELLE_AUTORISATION))
    )
  }

  # Libellé type logement
  if ("TYPE_PRINCIP_LOGTS_CREES" %in% names(df)) {
    df <- df %>% mutate(TYPE_LGT_LIBELLE = case_when(
      TYPE_PRINCIP_LOGTS_CREES == 1 ~ "Individuel pur",
      TYPE_PRINCIP_LOGTS_CREES == 2 ~ "Individuel groupé",
      TYPE_PRINCIP_LOGTS_CREES == 3 ~ "Collectif",
      TYPE_PRINCIP_LOGTS_CREES == 4 ~ "En résidence",
      TRUE ~ "Autre"
    ))
  }

  return(df)
}


# ════════════════════════════════════════════════════════════
# 4. Nettoyage locaux
# ════════════════════════════════════════════════════════════
nettoyer_locaux <- function(df) {

  for (col in intersect(names(df), c("DATE_REELLE_AUTORISATION",
                                      "DATE_REELLE_DOC",
                                      "DATE_REELLE_DAACT"))) {
    df[[col]] <- suppressWarnings(ymd(df[[col]]))
  }

  for (col in intersect(names(df), c(
    "SURF_LOC_CREEE", "SURF_LOC_DEMOLIE", "SURF_LOC_AVANT",
    "SURF_HAB_CREEE", "SURF_HEB_CREEE", "SURF_BUR_CREEE",
    "SURF_COM_CREEE", "SURF_ART_CREEE", "SURF_IND_CREEE",
    "SURF_AGR_CREEE", "SURF_ENT_CREEE", "SURF_PUB_CREEE",
    "NB_CHAMBRES", "SUPERFICIE_TERRAIN"
  ))) df[[col]] <- suppressWarnings(as.numeric(df[[col]]))

  if ("DATE_REELLE_AUTORISATION" %in% names(df)) {
    df <- df %>% mutate(
      ANNEE      = year(DATE_REELLE_AUTORISATION),
      MOIS       = month(DATE_REELLE_AUTORISATION),
      TRIMESTRE  = quarter(DATE_REELLE_AUTORISATION),
      ANNEE_MOIS = format(DATE_REELLE_AUTORISATION, "%Y-%m"),
      ANNEE_TRIM = paste0(year(DATE_REELLE_AUTORISATION), "-T",
                          quarter(DATE_REELLE_AUTORISATION))
    )
  }

  if ("DESTINATION_PRINCIPALE" %in% names(df)) {
    df <- df %>% mutate(DESTINATION_LIBELLE = case_when(
      DESTINATION_PRINCIPALE == 1 ~ "Habitation",
      DESTINATION_PRINCIPALE == 2 ~ "Hébergement hôtelier",
      DESTINATION_PRINCIPALE == 3 ~ "Bureau",
      DESTINATION_PRINCIPALE == 4 ~ "Commerce",
      DESTINATION_PRINCIPALE == 5 ~ "Artisanat",
      DESTINATION_PRINCIPALE == 6 ~ "Industrie",
      DESTINATION_PRINCIPALE == 7 ~ "Exploitation agricole",
      DESTINATION_PRINCIPALE == 8 ~ "Entrepôt",
      DESTINATION_PRINCIPALE == 9 ~ "Service public",
      TRUE ~ "Autre"
    ))
  }

  return(df)
}


# ════════════════════════════════════════════════════════════
# 5. GeoJSON communes 79
# ════════════════════════════════════════════════════════════
fetch_geojson_79 <- function() {
  cache_geo    <- file.path(CACHE_DIR, "communes_79.rds")
  geojson_path <- file.path(CACHE_DIR, "communes_79.geojson")

  if (file.exists(cache_geo)) {
    message("  🗺️  GeoJSON depuis cache")
    return(readRDS(cache_geo))
  }

  message("  🗺️  Téléchargement GeoJSON communes 79 ...")
  url <- "https://geo.api.gouv.fr/communes?codeDepartement=79&format=geojson&geometry=contour"
  tryCatch({
    geo <- st_read(url, quiet = TRUE)
    saveRDS(geo, cache_geo)
    st_write(geo, geojson_path, delete_dsn = TRUE, quiet = TRUE)
    message("  → ", nrow(geo), " communes")
    return(geo)
  }, error = function(e) {
    message("  ⚠️  GeoJSON : ", e$message)
    return(NULL)
  })
}

cache_est_perime <- function(fichier) {
  if (!file.exists(fichier)) return(TRUE)
  age <- as.numeric(difftime(Sys.time(), file.mtime(fichier), units = "days"))
  message("  Cache ", ifelse(age > 30, "périmé", "valide"),
          " (", round(age, 1), " jours)")
  return(age > 30)
}


# ════════════════════════════════════════════════════════════
# 6. Pipeline principal
# ════════════════════════════════════════════════════════════
charger_donnees_sitadel <- function(forcer_refresh = FALSE) {

  cache_lgt <- file.path(CACHE_DIR, "sitadel_logements_79.rds")
  cache_loc <- file.path(CACHE_DIR, "sitadel_locaux_79.rds")
  resultats <- list()

  # ── LOGEMENTS ──────────────────────────────────────────
  message("\n── LOGEMENTS ────────────────────────────────────────")
  if (forcer_refresh || cache_est_perime(cache_lgt)) {
    millesime <- get_dernier_millesime(RID_LOGEMENTS)
    df <- tryCatch(
      fetch_et_filtrer_dep79(RID_LOGEMENTS, millesime, "logements"),
      error = function(e) { message("❌ ", e$message); data.frame() }
    )
    if (nrow(df) > 0) {
      df <- nettoyer_logements(df)
      saveRDS(df, cache_lgt)
      message("✅ ", nrow(df), " lignes logements sauvegardées → cache")
    }
    resultats$logements <- df

  } else {
    resultats$logements <- readRDS(cache_lgt)
    message("✅ ", nrow(resultats$logements), " logements depuis cache")
  }

  # ── LOCAUX ─────────────────────────────────────────────
  message("\n── LOCAUX ───────────────────────────────────────────")
  if (forcer_refresh || cache_est_perime(cache_loc)) {
    millesime <- get_dernier_millesime(RID_LOCAUX)
    df <- tryCatch(
      fetch_et_filtrer_dep79(RID_LOCAUX, millesime, "locaux"),
      error = function(e) { message("❌ ", e$message); data.frame() }
    )
    if (nrow(df) > 0) {
      df <- nettoyer_locaux(df)
      saveRDS(df, cache_loc)
      message("✅ ", nrow(df), " lignes locaux sauvegardées → cache")
    }
    resultats$locaux <- df

  } else {
    resultats$locaux <- readRDS(cache_loc)
    message("✅ ", nrow(resultats$locaux), " locaux depuis cache")
  }

  # ── GEOJSON ────────────────────────────────────────────
  resultats$geo <- fetch_geojson_79()

  # ── Résumé ─────────────────────────────────────────────
  message("\n=== Résumé ===")
  if (!is.null(resultats$logements) && nrow(resultats$logements) > 0) {
    annees <- sort(unique(na.omit(resultats$logements$ANNEE)))
    message("Logements : ", nrow(resultats$logements), " lignes | ",
            n_distinct(resultats$logements$COMM), " communes | ",
            min(annees), "–", max(annees))
  }
  if (!is.null(resultats$locaux) && nrow(resultats$locaux) > 0) {
    message("Locaux    : ", nrow(resultats$locaux), " lignes | ",
            n_distinct(resultats$locaux$COMM), " communes")
  }

  message("=== Terminé — ", Sys.time(), " ===\n")
  return(resultats)
}


# ════════════════════════════════════════════════════════════
# EXÉCUTION
#
# ⚠️  PREMIER LANCEMENT : 10 à 20 min (téléchargement ~400 Mo)
#    Branchez votre ordinateur et lancez avant une pause.
#
# ✅  LANCEMENTS SUIVANTS : < 1 seconde (cache local RDS)
#    Le cache est renouvelé automatiquement si > 30 jours.
#
# Pour forcer un re-téléchargement :
#    charger_donnees_sitadel(forcer_refresh = TRUE)
# ════════════════════════════════════════════════════════════
donnees <- charger_donnees_sitadel(forcer_refresh = TRUE)