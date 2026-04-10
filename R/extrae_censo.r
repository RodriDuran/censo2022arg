# =============================================================================
# extrae_censo.R
# Pipeline de extraccion de microdatos del Censo Nacional 2022 desde REDATAM
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
# Este script implementa el proceso completo de extraccion provincia por
# provincia, con gestion eficiente de memoria y verificacion automatica
# de integridad contra los totales oficiales del INDEC.
#
# Funciones exportadas:
#   extraer_redatam() - funcion principal, orquesta todo el proceso
#
# Funciones internas (no exportadas):
#   construir_bloques()        - divide variables en grupos manejables
#   reconstruir_ids()          - genera identificadores de vivienda y hogar
#   log_msg()                  - registro de eventos con marca de tiempo
#   progreso()                 - barra de progreso en consola
#   extraer_bloque()           - extrae un bloque via subproceso
#   validar_bloques()          - verifica coherencia entre bloques
#   eliminar_ctrl_duplicados() - limpia columnas repetidas entre bloques
#   guardar_df()               - guarda en el formato solicitado
#   separar_bases()            - genera bases Personas, Hogares, Viviendas
#   verificar_provincia()      - verifica totales contra el INDEC
# =============================================================================

# =============================================================================
# Constantes del pipeline
# =============================================================================

# Variables de control que se incluyen en TODOS los bloques de extraccion.
# Su presencia en cada bloque permite verificar que el motor devolvio
# las personas en el mismo orden en todas las extracciones.
.VARS_CONTROL <- c("P01", "P02", "EDAD", "RADIO.REDCODEN")


# =============================================================================
# FUNCION INTERNA: construir_bloques()
#
# El motor REDATAM tiene un limite practico en la cantidad de variables
# que puede procesar en una sola consulta. Para superarlo, dividimos todas
# las variables del diccionario en grupos (bloques) de tamano controlado.
#
# Cada bloque incluye las variables de control para validacion cruzada.
# El primer bloque tambien incluye variables jerarquicas necesarias para
# reconstruir los identificadores de vivienda y hogar.
# =============================================================================
construir_bloques <- function(dic_vp, max_por_bloque = 10) {

  # Obtener todas las variables disponibles por entidad jerarquica
  vars_persona  <- redatam_variables(dic_vp, "PERSONA")$name
  vars_hogar    <- paste0("HOGAR.",    redatam_variables(dic_vp, "HOGAR")$name)
  vars_vivienda <- paste0("VIVIENDA.", redatam_variables(dic_vp, "VIVIENDA")$name)

  # Variables geograficas que identifican el radio censal
  vars_geo <- c("IDPROV", "IDPTO", "IDFRAC", "IDRADIO", "RADIO.REDCODEN")

  # Variables jerarquicas que solo van en el primer bloque:
  # TOTPOBV = total de personas por vivienda (necesario para reconstruir IDs)
  # V06     = tipo de vivienda
  # TOTPOBH = total de personas por hogar
  vars_jerarquicas <- c("VIVIENDA.TOTPOBV", "VIVIENDA.V06", "HOGAR.TOTPOBH")

  # Variables de control que van en todos los bloques (no computan para el maximo)
  ctrl_base <- c(.VARS_CONTROL, "IDPROV")

  # Armar el conjunto total de variables sustantivas sin duplicar controles
  todas_sustantivas <- unique(c(vars_persona, vars_hogar, vars_vivienda, vars_geo))
  todas_sustantivas <- setdiff(todas_sustantivas, c(ctrl_base, vars_jerarquicas))

  # Dividir en grupos de max_por_bloque
  n       <- length(todas_sustantivas)
  indices <- split(seq_len(n), ceiling(seq_len(n) / max_por_bloque))

  bloques <- lapply(seq_along(indices), function(i) {
    vars_bloque <- todas_sustantivas[indices[[i]]]
    if (i == 1) {
      # Bloque 1: control + variables jerarquicas + primeras sustantivas
      c(ctrl_base, vars_jerarquicas, vars_bloque)
    } else {
      # Bloques siguientes: solo control + sustantivas
      c(ctrl_base, vars_bloque)
    }
  })

  names(bloques) <- paste0("b", seq_along(bloques))

  message("Bloques construidos: ", length(bloques))
  for (nom in names(bloques)) {
    message("  ", nom, ": ", length(bloques[[nom]]), " variables")
  }

  return(bloques)
}

# =============================================================================
# FUNCION INTERNA: reconstruir_ids()
#
# La consulta TABLE VIEW de REDATAM devuelve los datos en formato plano, sin
# identificadores explicitos de vivienda ni de hogar. Para reconstruirlos
# se aprovechan dos variables del diccionario:
#
#   TOTPOBV (total de personas en la vivienda): es constante para todas
#   las personas de una misma vivienda. Al acumular personas y detectar
#   cuando se completa cada vivienda, se asigna un numero secuencial.
#
#   P01 (relacion con el/la jefe/a del hogar): el valor 1 indica jefe/a,
#   que siempre es la primera persona registrada en un hogar. Cada vez
#   que aparece un valor 1, comienza un nuevo hogar.
#
# Las claves resultantes son compuestas: codigo de provincia + numero
# secuencial, lo que garantiza unicidad a nivel nacional.
# =============================================================================

reconstruir_ids <- function(df) {
  if (!"parent"  %in% names(df)) stop("Se requiere columna 'parent'.")
  if (!"totpobv" %in% names(df)) stop("Se requiere columna 'totpobv'.")
  if (!"idprov"  %in% names(df)) stop("Se requiere columna 'idprov'.")

  df$totpobv <- as.integer(df$totpobv)

  # Asignar numero secuencial de vivienda usando TOTPOBV como contador
  id_viv_vec    <- integer(nrow(df))
  id_viv        <- 0L
  personas_acum <- 0L
  personas_viv  <- 0L

  for (i in seq_len(nrow(df))) {
    if (personas_acum == 0L) {
      id_viv       <- id_viv + 1L
      personas_viv <- df$totpobv[i]
    }
    id_viv_vec[i]  <- id_viv
    personas_acum  <- personas_acum + 1L
    if (personas_acum >= personas_viv) personas_acum <- 0L
  }

  df$num_vivienda <- id_viv_vec

  # Asignar numero secuencial de hogar usando P01 == 1 como senal de inicio
  df$num_hogar <- cumsum(df$parent == 1L)

  # Construir claves compuestas unicas a nivel nacional
  prov             <- df$idprov
  df$clave_vivienda <- paste0(prov, "_", df$num_vivienda)
  df$clave_hogar    <- paste0(
    prov, "_", df$num_vivienda, "_",
    ave(df$num_hogar, df$num_vivienda, FUN = function(x) match(x, unique(x)))
  )

  invisible(df)
}


# =============================================================================
# FUNCION INTERNA: log_msg()
# Registra un mensaje con marca de tiempo en consola y en el archivo de log.
# Los niveles disponibles son INFO, WARN y ERROR.
# =============================================================================
log_msg <- function(msg, log_file, nivel = "INFO") {
  linea <- paste0("[", format(Sys.time(), "%H:%M:%S"), "] [", nivel, "] ", msg)
  cat(linea, "\n")
  cat(linea, "\n", file = log_file, append = TRUE)
}


# =============================================================================
# FUNCION INTERNA: progreso()
# Muestra una barra de progreso simple en la consola.
# =============================================================================
progreso <- function(actual, total, etiqueta = "") {
  pct    <- round(actual / total * 100)
  llenos <- round(pct / 2)
  barra  <- paste0(
    "[", paste(rep("=", llenos),        collapse = ""),
         paste(rep(" ", 50 - llenos),   collapse = ""), "]"
  )
  cat(sprintf("\r%s %d/%d (%d%%) %s", barra, actual, total, pct, etiqueta))
  if (actual == total) cat("\n")
  flush.console()
}


# =============================================================================
# FUNCION INTERNA: extraer_bloque()
#
# Lanza un subproceso Rscript independiente para extraer un bloque de
# variables de una provincia. El uso de subprocesos es fundamental para
# la gestion de memoria: el motor REDATAM acumula memoria en el proceso
# de R que no puede ser liberada por el recolector de basura. Al finalizar
# cada subproceso, el sistema operativo libera toda esa memoria.
#
# Si el archivo de salida ya existe (extraccion anterior interrumpida),
# el bloque se salta para permitir la reanudacion del proceso.
# =============================================================================
extraer_bloque <- function(dic_path, spc, prov_cod, out_file,
                           log_file, nom_bloque, prov_nom) {

  # Reanudar extraccion: si el bloque ya existe no se vuelve a procesar
  if (file.exists(out_file)) {
    log_msg(paste0("Bloque ", nom_bloque, " (", prov_nom,
                   "): ya existe, saltando"), log_file)
    return(TRUE)
  }

  # Normalizar rutas para que sean validas dentro del script del subproceso
  dic_path <- normalizePath(dic_path, winslash = "/", mustWork = FALSE)
  out_file <- normalizePath(out_file, winslash = "/", mustWork = FALSE)

  # Script que se ejecutara en el subproceso
  script <- sprintf('
suppressMessages(library(redatamx))
suppressMessages(library(censo2022arg))
tryCatch({
  dic <- redatam_open("%s")
  # redatam_query_filtered: extrae solo los registros de la provincia indicada
  rts <- getDLLRegisteredRoutines("censo2022arg")$".Call"
  fn  <- rts[["_censo2022arg_redatam_query_filtered"]]
  df  <- as.data.frame(.Call(fn, dic, "%s", "IDPROV", %dL))
  saveRDS(df, "%s")
  cat("OK", nrow(df), "filas\\n")
}, error = function(e) {
  cat("ERROR:", conditionMessage(e), "\\n")
  quit(status = 1)
})

', dic_path, spc, prov_cod, out_file)

  tmp_script <- tempfile(fileext = ".R", tmpdir = dirname(out_file))
  on.exit(unlink(tmp_script), add = TRUE)
  writeLines(script, tmp_script)

#####PRUEBA BORRAR
  cat("=== SCRIPT DEL SUBPROCESO ===\n")
  cat(readLines(tmp_script), sep = "\n")
  cat("=== FIN SCRIPT ===\n")
#####FIN DE PRUEBA BORRAR

  output <- system(paste("Rscript", shQuote(tmp_script), "2>&1"), intern = TRUE, wait = TRUE)
  ret    <- attr(output, "status")
  ret    <- if (is.null(ret)) 0L else ret

  for (linea in output) log_msg(paste0("  [Rscript] ", linea), log_file)

  if (ret != 0 || !file.exists(out_file)) {
    log_msg(paste0("FALLO bloque ", nom_bloque,
                   " provincia ", prov_nom), log_file, "ERROR")
    return(FALSE)
  }

  sz <- round(file.size(out_file) / 1024 / 1024, 1)
  log_msg(paste0("OK bloque ", nom_bloque, " (", prov_nom,
                 ") - ", sz, " MB en disco"), log_file)
  return(TRUE)
}


# =============================================================================
# FUNCION INTERNA: validar_bloques()
#
# Verifica que el motor devolvio las personas en el mismo orden en dos
# bloques distintos, comparando las variables de control. Si el orden
# difiere, la union de columnas seria incorrecta.
# =============================================================================
validar_bloques <- function(df_base, df_nuevo, nom_bloque,
                            prov_nom, log_file) {

  # Verificar cantidad de filas
  if (nrow(df_base) != nrow(df_nuevo)) {
    log_msg(paste0("ERROR validacion bloque ", nom_bloque,
                   " (", prov_nom, "): filas base=", nrow(df_base),
                   " nuevo=", nrow(df_nuevo)), log_file, "ERROR")
    return(FALSE)
  }

  # Verificar que los valores de cada variable de control coinciden exactamente
  patrones <- list(
    P01      = "^p01_",
    P02      = "^p02_",
    EDAD     = "^edad_[0-9]",
    REDCODEN = "^redcoden_"
  )

  ok <- TRUE
  for (nom_ctrl in names(patrones)) {
    col_b <- names(df_base)[grepl(patrones[[nom_ctrl]], names(df_base),  ignore.case = TRUE)][1]
    col_n <- names(df_nuevo)[grepl(patrones[[nom_ctrl]], names(df_nuevo), ignore.case = TRUE)][1]
    if (is.na(col_b) || is.na(col_n)) next
    if (!identical(df_base[[col_b]], df_nuevo[[col_n]])) {
      log_msg(paste0("ADVERTENCIA: variable de control ", nom_ctrl,
                     " no coincide en bloque ", nom_bloque,
                     " (", prov_nom, ")"), log_file, "WARN")
      ok <- FALSE
    }
  }

  if (ok) log_msg(paste0("Validacion bloque ", nom_bloque, " (", prov_nom, "): OK"), log_file)
  return(ok)
}


# =============================================================================
# FUNCION INTERNA: eliminar_ctrl_duplicados()
#
# Al unir bloques con cbind(), las variables de control (presentes en todos
# los bloques) quedarian duplicadas. Esta funcion las elimina del bloque
# nuevo antes de unirlo, conservandolas solo en el bloque base.
# =============================================================================
eliminar_ctrl_duplicados <- function(df) {
  patron    <- "^p01_|^p02_|^edad_[0-9]|^redcoden_|^idprov_"
  cols_ctrl <- names(df)[grepl(patron, names(df), ignore.case = TRUE)]
  df[, !names(df) %in% cols_ctrl, drop = FALSE]
}


# =============================================================================
# FUNCION INTERNA: guardar_df()
#
# Guarda el data.frame en los formatos solicitados. El formato por defecto
# es parquet con compresion snappy, que ofrece un buen balance entre
# velocidad de lectura y tamano en disco.
#
# Nota: zstd no esta disponible en todas las versiones de Arrow; se usa
# snappy como alternativa portable.
# =============================================================================
guardar_df <- function(df, base_path, formatos = "parquet", log_file) {

  for (fmt in formatos) {
    tryCatch({
      out_path <- paste0(base_path, ".", fmt)
      if (fmt == "parquet") {
        arrow::write_parquet(df, out_path, compression = "snappy")
      } else if (fmt == "csv") {
        data.table::fwrite(df, out_path)
      } else if (fmt == "sav") {
        haven::write_sav(df, out_path)
      } else if (fmt == "sas") {
        haven::write_sas(df, paste0(base_path, ".sas7bdat"))
      }
      sz <- round(file.size(out_path) / 1024 / 1024, 1)
      log_msg(paste0("Guardado: ", basename(out_path), " (", sz, " MB)"), log_file)
    }, error = function(e) {
      log_msg(paste0("ERROR guardando ", fmt, ": ", e$message), log_file, "ERROR")
    })
  }
}


# =============================================================================
# FUNCION INTERNA: separar_bases()
#
# A partir de la base completa (una fila por persona), genera tres bases
# derivadas con distinto nivel de analisis:
#
#   Personas  - una fila por persona, con todas las variables de persona
#   Hogares   - una fila por hogar (primera persona del hogar), variables de hogar
#   Viviendas - una fila por vivienda (primera persona), variables de vivienda
#
# La seleccion de columnas se basa en patrones de nombre que corresponden
# a la nomenclatura del diccionario REDATAM del INDEC.
# =============================================================================
separar_bases <- function(df_completo, prov_dir, prov_nom,
                          formatos, log_file) {

  # Base Personas: todas las variables de persona con sus claves
  cols_persona <- c(
    "idprov", "num_vivienda", "num_hogar", "clave_vivienda", "clave_hogar",
    names(df_completo)[grepl(
      "^p0[0-9]_|^p[1-3][0-9]_|^edad|^conasi|^niv_asi|^aesc|^mni|^condact|^descapor|^hnvua|^letra|^po_",
      names(df_completo), ignore.case = TRUE
    )]
  )
  cols_persona <- intersect(cols_persona, names(df_completo))
  df_personas  <- df_completo[, cols_persona, drop = FALSE]
  guardar_df(df_personas,
             file.path(prov_dir, paste0(prov_nom, "_Personas")),
             formatos, log_file)
  rm(df_personas); gc()

  # Base Hogares: una fila por hogar, variables de nivel hogar
  cols_hogar <- c(
    "idprov", "num_vivienda", "num_hogar", "clave_vivienda", "clave_hogar",
    names(df_completo)[grepl(
      "^nhogh_|^totpobh_|^h1[0-9]_|^h2[0-9]_|^h[0-9]_|^nbi_|^tiphogar_|^eduhog_|^hogmig_|^htotmig_|^ipmh_",
      names(df_completo), ignore.case = TRUE
    )]
  )
  cols_hogar <- intersect(cols_hogar, names(df_completo))
  df_hogares <- df_completo[!duplicated(df_completo$clave_hogar),
                            cols_hogar, drop = FALSE]
  guardar_df(df_hogares,
             file.path(prov_dir, paste0(prov_nom, "_Hogares")),
             formatos, log_file)
  rm(df_hogares); gc()

  # Base Viviendas: una fila por vivienda, variables de nivel vivienda
  cols_vivienda <- c(
    "idprov", "num_vivienda", "clave_vivienda",
    names(df_completo)[grepl(
      "^tipovivg_|^v0[0-9]_|^codgl_|^codloc_|^codaglo_|^totpobv_|^tipo_radio_|^catgl_|^urp_",
      names(df_completo), ignore.case = TRUE
    )]
  )
  cols_vivienda <- intersect(cols_vivienda, names(df_completo))
  df_viviendas  <- df_completo[!duplicated(df_completo$clave_vivienda),
                               cols_vivienda, drop = FALSE]
  guardar_df(df_viviendas,
             file.path(prov_dir, paste0(prov_nom, "_Viviendas")),
             formatos, log_file)
  rm(df_viviendas); gc()
}


# =============================================================================
# FUNCION PRINCIPAL: extraer_redatam()
# =============================================================================

#' Extraer microdatos del Censo Nacional de Poblacion, Hogares y Viviendas 2022
#'
#' @description
#' Extrae los microdatos completos del Censo 2022 desde las bases REDATAM
#' del INDEC, provincia por provincia, con verificacion automatica de
#' integridad contra los totales oficiales publicados.
#'
#' Para cada provincia genera cinco archivos:
#' \itemize{
#'   \item \code{provincia_PO_VP.parquet} - base completa (una fila por persona)
#'   \item \code{provincia_Personas.parquet} - variables de persona
#'   \item \code{provincia_Hogares.parquet} - variables de hogar
#'   \item \code{provincia_Viviendas.parquet} - variables de vivienda
#'   \item \code{provincia_colectivas.parquet} - viviendas colectivas
#' }
#'
#' La extraccion es retomable: si se interrumpe, al volver a ejecutar
#' continua desde donde quedo sin repetir lo ya procesado.
#'
#' @param provincias Numerico o \code{"all"}. Codigos de provincia a extraer.
#'   Ejemplo: \code{c(34, 66)} extrae Formosa y Salta.
#'   Con \code{"all"} (default) procesa las 24 provincias.
#' @param formatos Character. Formato de salida de los archivos.
#'   Default \code{"parquet"}. Tambien acepta \code{"csv"}, \code{"sav"},
#'   \code{"sas"} o combinaciones: \code{c("parquet", "csv")}.
#' @param max_por_bloque Integer. Numero maximo de variables por bloque
#'   de extraccion. Default 10. Reducir en equipos con poca memoria RAM.
#' @param dic_path_vp Character. Ruta al archivo .rxdb de la Base VP.
#'   Si es \code{NULL} (default), se usa la ruta configurada con
#'   \code{censo_configurar()}.
#' @param dic_path_po Character. Ruta al archivo .rxdb de la Base PO.
#'   Si es \code{NULL} (default), se usa la ruta configurada.
#' @param dic_path_vc Character. Ruta al archivo .rxdb de la Base VC.
#'   Si es \code{NULL} (default), se usa la ruta configurada.
#' @param output_dir Character. Directorio de salida para los microdatos.
#'   Si es \code{NULL} (default), se usa el directorio configurado.
#' @param control_dir Character. Directorio con los archivos de control
#'   del INDEC para verificacion. Si es \code{NULL} (default), se usa
#'   el directorio configurado.
#'
#' @details
#' ## Bases del Censo 2022
#'
#' El INDEC distribuyo tres bases complementarias:
#' \itemize{
#'   \item \strong{VP} (Viviendas Particulares): contiene las variables
#'     principales e incluye los niveles geograficos de fraccion y radio
#'     censal, necesarios para analisis a nivel de radio.
#'   \item \strong{PO} (Pueblos Originarios, Afrodescendientes e Identidad
#'     de Genero): agrega seis variables sensibles no incluidas en VP.
#'     No incluye fraccion ni radio censal.
#'   \item \strong{VC} (Viviendas Colectivas): personas en establecimientos
#'     colectivos (hospitales, carceles, etc.).
#' }
#'
#' La base extraida combina VP y PO, obteniendo el radio censal de VP
#' y las seis variables adicionales de PO.
#'
#' ## Gestion de memoria
#'
#' Cada bloque se extrae en un subproceso Rscript independiente. Esto
#' evita la acumulacion de memoria del motor REDATAM en la sesion
#' principal de R. El proceso maestro solo lee los archivos intermedios
#' desde disco y los une en memoria.
#'
#' @return Invisible. Una lista con los elementos:
#'   \code{fallidas} (provincias con errores) y \code{log} (ruta al log).
#'
#' @examples
#' \dontrun{
#' # Extraer todas las provincias (configuracion automatica de rutas)
#' extraer_redatam()
#'
#' # Extraer solo Formosa para probar
#' extraer_redatam(provincias = 34)
#'
#' # Extraer varias provincias
#' extraer_redatam(provincias = c(34, 66, 54))
#'
#' # Con rutas propias (sin usar censo_configurar)
#' extraer_redatam(
#'   dic_path_vp = "/mi/ruta/Base_VP/cpv2022.rxdb",
#'   dic_path_po = "/mi/ruta/Base_PO/cpv2022.rxdb",
#'   dic_path_vc = "/mi/ruta/Base_VC/cpv2022col.rxdb",
#'   output_dir  = "/mi/ruta/microdatos/"
#' )
#' }
#'
#' @seealso
#' \code{\link{censo_configurar}} para configurar las rutas,
#' \code{\link{censo_etiquetar}} para etiquetar los archivos generados,
#' \code{\link{censo_leer}} para leer los microdatos extraidos.
#'
#' @export
extraer_redatam <- function(
    provincias     = "all",
    formatos       = "parquet",
    max_por_bloque = 10,
    dic_path_vp    = NULL,
    dic_path_po    = NULL,
    dic_path_vc    = NULL,
    output_dir     = NULL,
    control_dir    = NULL
) {

  # Resolver rutas: usar las configuradas si no se especifican explicitamente
  dic_path_vp <- if (!is.null(dic_path_vp)) dic_path_vp else censo_rxdb_vp()
  dic_path_po <- if (!is.null(dic_path_po)) dic_path_po else censo_rxdb_po()
  dic_path_vc <- if (!is.null(dic_path_vc)) dic_path_vc else censo_rxdb_vc()
  output_dir  <- if (!is.null(output_dir))  output_dir  else censo_dir_microdatos()
  control_dir <- if (!is.null(control_dir)) control_dir else censo_dir_control()

  # Verificar que los archivos de las bases existen antes de comenzar
  for (f in c(dic_path_vp, dic_path_po, dic_path_vc)) {
    if (!file.exists(f))
      stop("Archivo no encontrado: ", f,
           "\nDescargue las bases con censo_descargar() o verifique la ruta.")
  }

  # Crear directorios de salida
  dir.create(file.path(output_dir, "provincias"), showWarnings = FALSE, recursive = TRUE)
  dir.create(file.path(output_dir, "logs"),       showWarnings = FALSE, recursive = TRUE)

  log_file <- file.path(output_dir, "logs",
                        paste0("extraccion_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log"))

  log_msg("=== INICIO EXTRACCION CENSO 2022 ===", log_file)
  log_msg(paste("Output:", output_dir), log_file)
  log_msg(paste("Formatos:", paste(formatos, collapse = ", ")), log_file)
  log_msg(paste("Max vars por bloque:", max_por_bloque), log_file)

  # Validar directorio de control (contiene totales oficiales del INDEC)
  if (!dir.exists(control_dir)) {
    log_msg(paste0("Directorio de control no encontrado: ", control_dir,
                   " - verificacion de integridad desactivada"), log_file, "WARN")
    control_dir <- NULL
  } else {
    log_msg(paste0("Tablas de control: ", control_dir), log_file)
  }

  # -------------------------------------------------------------------------
  # Preparacion con diccionarios abiertos en sesion maestra
  # Los diccionarios deben cerrarse ANTES de lanzar subprocesos; de lo
  # contrario los subprocesos no pueden abrirlos (bloqueo de archivo).
  # -------------------------------------------------------------------------

  # Obtener la lista de provincias desde la base VP
  log_msg("Obteniendo lista de provincias...", log_file)
  dic_vp <- redatam_open(dic_path_vp)
  tmp_spc <- tempfile(fileext = ".spc")
  on.exit(unlink(tmp_spc), add = TRUE)
  writeLines("AREALIST PROV, IDPROV, NPROV", tmp_spc)
  tmp_prov      <- redatam_run(dic_vp, tmp_spc)
  prov_raw      <- if (is.data.frame(tmp_prov)) tmp_prov else tmp_prov[[1]]
  idprov_col    <- names(prov_raw)[grepl("idprov", names(prov_raw), ignore.case = TRUE)][1]
  nprov_col     <- names(prov_raw)[grepl("nprov",  names(prov_raw), ignore.case = TRUE)][1]
  provincias_df <- data.frame(
    codigo = as.integer(prov_raw[[idprov_col]]),
    nombre = tolower(gsub("[^a-zA-Z0-9]", "_", prov_raw[[nprov_col]]))
  )
  rm(tmp_prov, prov_raw); gc()

  # Filtrar provincias si se especifico un subconjunto
  if (!identical(provincias, "all")) {
    provincias_df <- provincias_df[provincias_df$codigo %in% provincias, ]
    if (nrow(provincias_df) == 0)
      stop("Ninguna de las provincias indicadas fue encontrada en el diccionario.")
  }
  log_msg(paste("Provincias a procesar:", nrow(provincias_df)), log_file)

  # Construir bloques de variables desde VP
  log_msg("Construyendo bloques de variables VP...", log_file)
  BLOQUES_VP <- construir_bloques(dic_vp, max_por_bloque)
  log_msg(paste("Total bloques VP:", length(BLOQUES_VP)), log_file)

  # Preparar consulta para la Base PO (solo 6 variables adicionales + controles)
  # PO no tiene FRAC ni RADIO - estas variables vienen exclusivamente de VP
  dic_po_tmp  <- redatam_open(dic_path_po)
  vars_po_all <- redatam_variables(dic_po_tmp, "PERSONA")$name
  vars_po_exc <- c("P01", "P02", "EDAD", "TOTPOBV", "TOTPOBH", "V06",
                   "IDPROV", "IDPTO", "IDFRAC", "IDRADIO")
  vars_po_add <- setdiff(vars_po_all, vars_po_exc)  # solo las 6 variables adicionales
  spc_po      <- paste("TABLE VIEW PERSONA",
                       paste(c(.VARS_CONTROL, vars_po_add), collapse = ", "))
  redatam_close(dic_po_tmp); rm(dic_po_tmp)

  # Preparar consulta para la Base VC (viviendas colectivas)
  dic_vc_tmp  <- redatam_open(dic_path_vc)
  vars_vc_viv <- redatam_variables(dic_vc_tmp, "VIVIENDA")$name
  spc_vc      <- paste("TABLE VIEW VIVIENDA",
                       paste(c("IDPROV", vars_vc_viv), collapse = ", "))
  redatam_close(dic_vc_tmp); rm(dic_vc_tmp)

  # CRITICO: cerrar todos los diccionarios antes de lanzar subprocesos
  redatam_close(dic_vp)
  rm(dic_vp); gc()
  log_msg("Diccionarios cerrados. Iniciando subprocesos...", log_file)

  # -------------------------------------------------------------------------
  # FUNCION LOCAL: verificar_provincia()
  # Compara los totales extraidos con los publicados por el INDEC.
  # Se define aqui para tener acceso al log_file del contexto padre.
  # -------------------------------------------------------------------------
  verificar_provincia <- function(prov_dir, prov_nom, prov_cod,
                                  control_dir, log_file) {

    log_msg("Verificando contra totales de control INDEC...", log_file)

    # Cargar los totales oficiales del INDEC para esta provincia
    ctrl_pob <- read.csv(file.path(control_dir, "control_poblacion.csv"))
    ctrl_hog <- read.csv(file.path(control_dir, "control_hogares.csv"))
    ctrl_viv <- read.csv(file.path(control_dir, "control_viviendas.csv"))

    esp_personas  <- ctrl_pob$pob_viv_particulares[ctrl_pob$codigo == prov_cod]
    esp_hogares   <- ctrl_hog$total_hogares[ctrl_hog$codigo == prov_cod]
    esp_viviendas <- ctrl_viv$viv_part_habitadas[ctrl_viv$codigo == prov_cod]

    if (length(esp_personas) == 0) {
      log_msg(paste0("Codigo ", prov_cod,
                     " no encontrado en tablas de control"), log_file, "WARN")
      return(NULL)
    }

    # Leer solo las columnas de clave para minimizar el uso de RAM
    archivo <- file.path(prov_dir, paste0(prov_nom, "_PO_VP.parquet"))
    if (!file.exists(archivo)) {
      log_msg("Archivo PO_VP no encontrado para verificacion", log_file, "ERROR")
      return(NULL)
    }

    dt <- as.data.frame(arrow::read_parquet(
      archivo,
      col_select = c("clave_vivienda", "clave_hogar")
    ))

    n_personas  <- nrow(dt)
    n_hogares   <- length(unique(dt$clave_hogar))
    n_viviendas <- length(unique(dt$clave_vivienda))
    rm(dt); gc()

    resultados <- data.frame(
      verificacion = c("Personas (VP)", "Hogares", "Viviendas habitadas"),
      esperado     = c(esp_personas,  esp_hogares,  esp_viviendas),
      obtenido     = c(n_personas,    n_hogares,    n_viviendas),
      diferencia   = c(n_personas  - esp_personas,
                       n_hogares   - esp_hogares,
                       n_viviendas - esp_viviendas),
      ok           = c(n_personas  == esp_personas,
                       n_hogares   == esp_hogares,
                       n_viviendas == esp_viviendas),
      stringsAsFactors = FALSE
    )

    for (j in seq_len(nrow(resultados))) {
      nivel   <- if (resultados$ok[j]) "INFO" else "ERROR"
      simbolo <- if (resultados$ok[j]) "OK" else "FALLO"
      log_msg(paste0("  ", simbolo, " ", resultados$verificacion[j],
                     ": esperado=",  format(resultados$esperado[j],  big.mark = ","),
                     " | obtenido=", format(resultados$obtenido[j],  big.mark = ","),
                     " | dif=",      resultados$diferencia[j]),
              log_file, nivel)
    }

    # Guardar resultado de verificacion junto a los datos de la provincia
    out_verif <- file.path(prov_dir, paste0(prov_nom, "_verificacion.csv"))
    write.csv(resultados, out_verif, row.names = FALSE)
    log_msg(paste0("Verificacion guardada: ", basename(out_verif)), log_file)

    todo_ok <- all(resultados$ok)
    if (todo_ok) {
      log_msg("Verificacion COMPLETA: todos los totales coinciden", log_file)
    } else {
      log_msg(paste0("Verificacion FALLIDA: ",
                     sum(!resultados$ok), " discrepancia(s)"), log_file, "ERROR")
    }

    return(todo_ok)
  }


  # =========================================================================
  # BUCLE PRINCIPAL: procesar cada provincia
  # =========================================================================
  fallidas <- list()

  for (i in seq_len(nrow(provincias_df))) {
    prov_cod <- provincias_df$codigo[i]
    prov_nom <- provincias_df$nombre[i]
    prov_dir <- file.path(output_dir, "provincias",
                          paste0(sprintf("%02d", prov_cod), "_", prov_nom))
    tmp_dir  <- file.path(prov_dir, "tmp")

    # Saltar provincias ya completadas (archivo PO_VP presente)
    archivo_final <- file.path(prov_dir, paste0(prov_nom, "_PO_VP.parquet"))
    if (file.exists(archivo_final)) {
      log_msg(paste0("Provincia ", prov_nom, " ya completada, saltando"), log_file)
      next
    }

    # Crear o retomar directorios temporales
    if (!dir.exists(tmp_dir)) {
      dir.create(tmp_dir, showWarnings = FALSE, recursive = TRUE)
    } else {
      log_msg(paste0("Retomando ", prov_nom, " - bloques previos disponibles"), log_file)
    }
    dir.create(file.path(prov_dir, "colectivas"), showWarnings = FALSE)

    log_msg(paste0("\n>>> PROVINCIA ", i, "/", nrow(provincias_df),
                   ": ", toupper(prov_nom), " (", prov_cod, ") <<<"), log_file)

    # -----------------------------------------------------------------------
    # PASO 1: Extraer bloques VP en subprocesos
    # -----------------------------------------------------------------------
    log_msg("PASO 1/6: Extrayendo bloques VP...", log_file)
    n_bloques   <- length(BLOQUES_VP)
    archivos_ok <- c()

    for (j in seq_along(BLOQUES_VP)) {
      nom_b <- names(BLOQUES_VP)[j]
      spc_b <- paste("TABLE VIEW PERSONA",
                     paste(BLOQUES_VP[[nom_b]], collapse = ", "))
      out_b <- file.path(tmp_dir, paste0("vp_", nom_b, ".rds"))

      progreso(j, n_bloques, paste0("bloque ", nom_b))
      log_msg(paste0("Extrayendo ", nom_b, "/", n_bloques,
                     " (", prov_nom, ")"), log_file)

      ok <- extraer_bloque(dic_path_vp, spc_b, prov_cod,
                           out_b, log_file, nom_b, prov_nom)
      if (ok) {
        archivos_ok <- c(archivos_ok, setNames(out_b, nom_b))
      } else {
        fallidas[[length(fallidas) + 1]] <- list(
          tipo = "VP_bloque", bloque = nom_b,
          provincia = prov_nom, codigo = prov_cod)
      }
    }

    if (length(archivos_ok) == 0) {
      log_msg(paste0("ERROR CRITICO: sin bloques VP para ", prov_nom),
              log_file, "ERROR")
      unlink(tmp_dir, recursive = TRUE); gc()
      next
    }

    # -----------------------------------------------------------------------
    # PASO 2: Unir y validar bloques VP
    # -----------------------------------------------------------------------
    log_msg("PASO 2/6: Uniendo y validando bloques VP...", log_file)
    df_vp <- NULL

    for (j in seq_along(archivos_ok)) {
      nom_b  <- names(archivos_ok)[j]
      arch   <- archivos_ok[j]
      bloque <- readRDS(arch)

      if (nrow(bloque) == 0) {
        log_msg(paste0("Bloque ", nom_b, " vacio, saltando"), log_file, "WARN")
        rm(bloque); gc()
        file.remove(arch)
        next
      }

      if (is.null(df_vp)) {
        df_vp <- bloque
        log_msg(paste0("Bloque ", nom_b, " unido"), log_file)
        rm(bloque)
      } else {
        validar_bloques(df_vp, bloque, nom_b, prov_nom, log_file)
        bloque <- eliminar_ctrl_duplicados(bloque)
        df_vp  <- cbind(df_vp, bloque)
        log_msg(paste0("Bloque ", nom_b, " unido"), log_file)
        rm(bloque)
      }
      gc()
    }

    log_msg(paste0("VP completa: ", format(nrow(df_vp), big.mark = ","),
                   " filas x ", ncol(df_vp), " cols"), log_file)

    # -----------------------------------------------------------------------
    # PASO 3: Reconstruir identificadores jerarquicos
    # -----------------------------------------------------------------------
    log_msg("PASO 3/6: Reconstruyendo IDs...", log_file)

    # Crear alias temporales con nombres canonicos para reconstruir_ids()
    parent_col  <- names(df_vp)[grepl("^p01_",    names(df_vp), ignore.case = TRUE)][1]
    totpobv_col <- names(df_vp)[grepl("^totpobv_", names(df_vp), ignore.case = TRUE)][1]
    idprov_col  <- names(df_vp)[grepl("^idprov_",  names(df_vp), ignore.case = TRUE)][1]

    df_vp$parent  <- df_vp[[parent_col]]
    df_vp$totpobv <- as.integer(df_vp[[totpobv_col]])
    df_vp$idprov  <- df_vp[[idprov_col]]

    df_vp <- reconstruir_ids(df_vp)

    # Eliminar alias temporales
    df_vp$parent  <- NULL
    df_vp$totpobv <- NULL
    df_vp$idprov  <- NULL
    gc()

    log_msg(paste0("IDs reconstruidos: ",
                   format(length(unique(df_vp$clave_vivienda)), big.mark = ","),
                   " viviendas, ",
                   format(length(unique(df_vp$clave_hogar)), big.mark = ","),
                   " hogares"), log_file)

    # -----------------------------------------------------------------------
    # PASO 4: Extraer Base PO (6 variables adicionales)
    # -----------------------------------------------------------------------
    log_msg("PASO 4/6: Extrayendo PO...", log_file)
    out_po <- file.path(tmp_dir, "po_adicional.rds")
    ok_po  <- extraer_bloque(dic_path_po, spc_po, prov_cod,
                             out_po, log_file, "PO", prov_nom)

    if (ok_po) {
      df_po_add <- readRDS(out_po)
      file.remove(out_po)

      # Verificar que PO tiene el mismo numero de filas que VP
      validar_bloques(df_vp, df_po_add, "PO", prov_nom, log_file)

      # Agregar prefijo "po_" para distinguir estas variables en la base combinada
      cols_po <- setdiff(names(df_po_add), .VARS_CONTROL)
      names(df_po_add)[names(df_po_add) %in% cols_po] <-
        paste0("po_", names(df_po_add)[names(df_po_add) %in% cols_po])

      # Unir las 6 variables de PO a la base VP
      cols_nuevas <- setdiff(names(df_po_add), .VARS_CONTROL)
      df_vp       <- cbind(df_vp, df_po_add[, cols_nuevas, drop = FALSE])
      rm(df_po_add); gc()

      log_msg(paste0("PO: ", length(cols_nuevas), " variables adicionales"), log_file)
    } else {
      log_msg("ADVERTENCIA: extraccion PO fallo - base sin variables de identidad",
              log_file, "WARN")
      fallidas[[length(fallidas) + 1]] <- list(
        tipo = "PO", provincia = prov_nom, codigo = prov_cod)
    }

    # -----------------------------------------------------------------------
    # PASO 5: Guardar base completa y bases separadas
    # -----------------------------------------------------------------------
    log_msg("PASO 5/6: Guardando bases...", log_file)

    # Base completa VP+PO
    guardar_df(df_vp,
               file.path(prov_dir, paste0(prov_nom, "_PO_VP")),
               formatos, log_file)

    # Eliminar archivos temporales de bloques
    for (arch in archivos_ok) {
      if (file.exists(arch)) file.remove(arch)
    }
    log_msg("Bloques temporales eliminados del disco", log_file)

    # Generar bases Personas, Hogares, Viviendas
    separar_bases(df_vp, prov_dir, prov_nom, formatos, log_file)

    # Verificar integridad contra totales del INDEC
    if (!is.null(control_dir)) {
      verificar_provincia(prov_dir, prov_nom, prov_cod, control_dir, log_file)
    }

    rm(df_vp); gc()

    # -----------------------------------------------------------------------
    # PASO 6: Extraer Base VC (viviendas colectivas)
    # -----------------------------------------------------------------------
    log_msg("PASO 6/6: Extrayendo VC...", log_file)
    out_vc <- file.path(tmp_dir, "vc.rds")
    ok_vc  <- extraer_bloque(dic_path_vc, spc_vc, prov_cod,
                             out_vc, log_file, "VC", prov_nom)
    if (ok_vc) {
      df_vc <- readRDS(out_vc)
      file.remove(out_vc)
      guardar_df(df_vc,
                 file.path(prov_dir, "colectivas",
                           paste0(prov_nom, "_colectivas")),
                 formatos, log_file)
      rm(df_vc); gc()
    } else {
      log_msg("ADVERTENCIA: extraccion VC fallo", log_file, "WARN")
      fallidas[[length(fallidas) + 1]] <- list(
        tipo = "VC", provincia = prov_nom, codigo = prov_cod)
    }

    # Limpiar directorio temporal de la provincia
    unlink(tmp_dir, recursive = TRUE)
    gc()

    log_msg(paste0(">>> Provincia ", prov_nom, " completada (",
                   i, "/", nrow(provincias_df), ") <<<"), log_file)
  }

  # =========================================================================
  # RESUMEN FINAL
  # =========================================================================
  log_msg("\n=== RESUMEN FINAL ===", log_file)
  log_msg(paste("Provincias procesadas:", nrow(provincias_df)), log_file)
  if (length(fallidas) > 0) {
    log_msg(paste("Fallos registrados:", length(fallidas)), log_file, "WARN")
    for (f in fallidas) {
      log_msg(paste0("  - ", f$tipo,
                     if (!is.null(f$bloque)) paste0(" [", f$bloque, "]") else "",
                     "  ", f$provincia, " (", f$codigo, ")"), log_file)
    }
  } else {
    log_msg("Sin errores", log_file)
  }
  log_msg("=== FIN ===", log_file)

  invisible(list(fallidas = fallidas, log = log_file))
}
