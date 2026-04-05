# =============================================================================
# leer_censo.R
# Lectura de microdatos extraidos y etiquetados del Censo 2022 para analisis
# posterior
#
# Copyright (C) 2025 Rodrigo Javier Duran
# INENCO - CONICET / Universidad Nacional de Salta
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# If you use this package in your research, please cite it as:
#   Duran, R. J. (2026). censo2022arg: Herramientas para el analisis del
#   Censo Argentino 2022. INENCO - CONICET/UNSa.
#   https://doi.org/10.5281/zenodo.19422784
# =============================================================================
# Proporciona una interfaz unificada para leer los archivos generados por
# extraer_redatam() y etiquetados por censo_etiquetar(), con soporte para
# seleccion de provincias, columnas y filtros de filas aplicados antes
# de cargar los datos en memoria.
# =============================================================================


#' Leer microdatos del Censo 2022
#'
#' @description
#' Lee los microdatos extraidos del Censo 2022 y los devuelve como un
#' objeto de R listo para analizar. Permite seleccionar provincias,
#' variables y aplicar filtros de filas sin necesidad de cargar todo
#' el conjunto de datos en memoria.
#'
#' Se recomienda ejecutar \code{censo_etiquetar()} antes de usar esta
#' funcion para disponer de nombres de variable legibles y categorias
#' correctamente etiquetadas.
#'
#' @param base Character. Base de datos a leer:
#'   \itemize{
#'     \item \code{"Personas"} (default): una fila por persona
#'     \item \code{"Hogares"}: una fila por hogar
#'     \item \code{"Viviendas"}: una fila por vivienda particular
#'     \item \code{"colectivas"}: personas en viviendas colectivas
#'     \item \code{"PO_VP"}: base completa combinada (VP + PO)
#'   }
#' @param provincias Numerico, character o \code{"all"}. Provincias a leer.
#'   Acepta codigos numericos (\code{c(66, 38)}), nombres o partes del
#'   nombre (\code{c("salta", "jujuy")}), o \code{"all"} (default) para
#'   todas las provincias disponibles.
#'   \code{"all"} (default), se solicita confirmacion dado el volumen
#'   de datos (~12-15 GB en RAM para el pais completo).
#' @param columnas Character o \code{NULL}. Nombres de variables a leer.
#'   Si es \code{NULL} (default), se cargan todas las columnas.
#'   Ejemplo: \code{c("P01", "EDAD", "CONDACT")}.
#' @param filtro Expresion o \code{NULL}. Condicion para filtrar filas
#'   antes de cargarlas en memoria. Se evalua sobre el dataset completo
#'   sin necesidad de cargarlo. Ejemplo: \code{quote(EDAD >= 18)}.
#' @param formato Character. Formato del objeto devuelto:
#'   \code{"data.frame"} (default), \code{"data.table"} o \code{"tibble"}.
#' @param dir Character o \code{NULL}. Directorio raiz de los datos.
#'   Si es \code{NULL} (default), usa el directorio configurado con
#'   \code{censo_configurar()}.
#'
#' @details
#' ## Filtros y seleccion de columnas
#'
#' Los filtros y la seleccion de columnas se aplican usando Apache Arrow
#' antes de cargar los datos en la memoria de R. Esto permite trabajar
#' con subconjuntos de datos sin necesidad de disponer de la RAM suficiente
#' para el conjunto completo.
#'
#' ## Estimacion de uso de memoria
#'
#' Los archivos en disco estan comprimidos (formato parquet). Al cargarlos
#' en R, el uso de RAM es aproximadamente 4-5 veces mayor que el tamano
#' del archivo. Una provincia mediana ocupa ~50-100 MB en RAM. El pais
#' completo puede superar los 12 GB.
#'
#' @return Un \code{data.frame}, \code{data.table} o \code{tibble} segun
#'   el parametro \code{formato}.
#'
#' @examples
#' \dontrun{
#' # Personas de Formosa
#' df <- censo_leer(base = "Personas", provincias = 34)
#'
#' # Por nombre de provincia
#' df <- censo_leer(base = "Personas", provincias = c("salta", "jujuy"))
#'
#' # Personas de Salta y Jujuy, solo algunas variables
#' df <- censo_leer(
#'   base      = "Personas",
#'   provincias = c(66, 38),
#'   columnas  = c("P01", "EDAD", "CONDACT", "IDRADIO")
#' )
#'
#' # Personas mayores de 18 anos de Cordoba
#' df <- censo_leer(
#'   base       = "Personas",
#'   provincias = 14,
#'   filtro     = quote(EDAD >= 18)
#' )
#'
#' # Hogares de todo el pais como data.table
#' dt <- censo_leer(base = "Hogares", formato = "data.table")
#' }
#'
#' @seealso \code{\link{extraer_redatam}}, \code{\link{censo_etiquetar}},
#'   \code{\link{censo_configurar}}
#' @export
censo_leer <- function(
    base       = "Personas",
    provincias = "all",
    columnas   = NULL,
    filtro     = NULL,
    formato    = "data.frame",
    dir        = NULL
) {

  formato <- match.arg(formato, c("data.frame", "data.table", "tibble"))
  base    <- match.arg(base, c("Personas", "Hogares", "Viviendas", "colectivas", "PO_VP"))

  # Resolver el directorio de provincias
  if (is.null(dir)) dir <- censo_dir()
  dir_prov <- file.path(dir, "provincias")
  if (!dir.exists(dir_prov))
    stop("Directorio de provincias no encontrado: ", dir_prov,
         "\nVerifique la configuracion con censo_info().")

  # Obtener todas las carpetas de provincia disponibles
  # El nombre de cada carpeta sigue el patron "02_caba", "34_formosa", etc.
  todas_carpetas <- list.dirs(dir_prov, recursive = FALSE, full.names = TRUE)
  todas_carpetas <- todas_carpetas[grepl("^[0-9]{2}_", basename(todas_carpetas))]

  if (length(todas_carpetas) == 0)
    stop("No se encontraron provincias extraidas en: ", dir_prov,
         "\nEjecute primero extraer_redatam().")

  # Filtrar por provincia si se especifico un subconjunto
  if (!identical(provincias, "all")) {
    if (is.numeric(provincias)) {
      # Buscar por codigo numerico (ej: 66 -> "66_salta")
      patron <- paste0("^(", paste(sprintf("%02d", provincias), collapse = "|"), ")_")
      todas_carpetas <- todas_carpetas[grepl(patron, basename(todas_carpetas))]
    } else {
      # Buscar por nombre parcial
      todas_carpetas <- todas_carpetas[grepl(
        paste(tolower(provincias), collapse = "|"),
        basename(todas_carpetas), ignore.case = TRUE
      )]
    }
    if (length(todas_carpetas) == 0)
      stop("No se encontraron las provincias indicadas. ",
           "Verifique los codigos con censo_info().")
  }

  # Advertir si se van a cargar todas las provincias juntas
  if (identical(provincias, "all")) {
    message("[INFO] Se cargaran ", length(todas_carpetas),
            " provincias. El uso de memoria puede superar los 12 GB.")
    if (interactive()) {
      cat("[INFO] ?Desea continuar? (s/n): ")
      respuesta <- readline()
      if (tolower(trimws(respuesta)) != "s") {
        message("[INFO] Operacion cancelada.")
        return(invisible(NULL))
      }
    }
  }

  # Construir las rutas a los archivos parquet de cada provincia
  archivos <- sapply(todas_carpetas, function(carpeta) {
    prov_nom <- sub("^[0-9]+_", "", basename(carpeta))
    if (base == "colectivas") {
      file.path(carpeta, "colectivas", paste0(prov_nom, "_colectivas.parquet"))
    } else {
      file.path(carpeta, paste0(prov_nom, "_", base, ".parquet"))
    }
  })

  # Informar sobre archivos no encontrados (provincias sin esa base)
  faltantes <- archivos[!file.exists(archivos)]
  if (length(faltantes) > 0) {
    cat("[AVISO] Archivos no encontrados (se omitiran):\n")
    for (f in faltantes) cat("  -", basename(f), "\n")
    archivos <- archivos[file.exists(archivos)]
  }
  if (length(archivos) == 0)
    stop("No se encontraron archivos para leer. ",
         "Verifique que los datos fueron extraidos con extraer_redatam().")

  # Verificar si los archivos fueron etiquetados
  # Los archivos sin etiquetar tienen sufijos numericos en los nombres (ej: p01_0)
  noms_test <- names(arrow::read_parquet(archivos[1], as_data_frame = FALSE)$schema)
  if (any(grepl("_[0-9]+$", noms_test))) {
    message("[AVISO] Los archivos no han sido etiquetados.")
    message("[INFO]  Se recomienda ejecutar censo_etiquetar() para obtener",
            " nombres de variable legibles y categorias etiquetadas.")
    if (interactive()) {
      cat("[INFO]  ?Desea continuar de todas formas? (s/n): ")
      respuesta <- readline()
      if (tolower(trimws(respuesta)) != "s") {
        message("[INFO] Operacion cancelada.")
        return(invisible(NULL))
      }
    }
  }

  # ---- Leer y combinar provincias ------------------------------------------
  cat("[INFO] Leyendo", length(archivos), "provincia(s)...\n")

  lista <- vector("list", length(archivos))

  for (k in seq_along(archivos)) {
    arch     <- archivos[k]
    prov_nom <- sub("^[0-9]+_", "", basename(dirname(arch)))
    if (base == "colectivas")
      prov_nom <- sub("^[0-9]+_", "", basename(dirname(dirname(arch))))

    cat(sprintf("[INFO]  (%d/%d) %s\n", k, length(archivos), prov_nom))

    # Abrir el dataset de forma lazy (sin cargar en RAM todavia)
    ds <- arrow::open_dataset(arch)

    # Aplicar filtro de filas antes de cargar en memoria
    # Arrow evalua el filtro directamente sobre el archivo comprimido
    if (!is.null(filtro)) {
      ds <- dplyr::filter(ds, !!filtro)
    }

    # Seleccionar columnas antes de cargar en memoria
    if (!is.null(columnas)) {
      cols_disponibles <- names(ds$schema)
      cols_validas     <- columnas[columnas %in% cols_disponibles]
      cols_faltantes   <- columnas[!columnas %in% cols_disponibles]
      if (length(cols_faltantes) > 0)
        cat("[AVISO] Columnas no encontradas:",
            paste(cols_faltantes, collapse = ", "), "\n")
      if (length(cols_validas) > 0)
        ds <- dplyr::select(ds, dplyr::all_of(cols_validas))
    }

    # Ejecutar la consulta y traer los datos a memoria
    lista[[k]] <- dplyr::collect(ds)
    rm(ds); gc()
  }

  # Combinar todas las provincias en un unico objeto
  cat("[INFO] Combinando provincias...\n")
  df <- do.call(rbind, lista)
  rm(lista); gc()

  cat("[INFO] Total cargado:", format(nrow(df), big.mark = ","),
      "filas x", ncol(df), "columnas\n")

  # Convertir al formato solicitado
  if (formato == "data.table") {
    df <- data.table::as.data.table(df)
  } else if (formato == "tibble") {
    if (!requireNamespace("tibble", quietly = TRUE))
      stop("Para usar formato = 'tibble' instale el paquete: install.packages('tibble')")
    df <- tibble::as_tibble(df)
  }

  df
}
