# =============================================================================
# extraer_rxdb.R
# Extraccion generica de microdatos desde cualquier base RedatamX (.rxdb)
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
#   Duran, R. J. (2025). censo2022arg: Herramientas para el analisis del
#   Censo Argentino 2022. INENCO - CONICET/UNSa.
#   https://github.com/RodriDuran/censo2022arg
# =============================================================================
# Esta funcion es el componente de proposito general del paquete. Mientras
# que extraer_redatam() implementa el pipeline completo y especifico para
# el Censo Argentina 2022 (con filtrado provincial, reconstruccion de IDs
# y verificacion de integridad), extraer_rxdb() opera como un convertidor
# universal: recibe cualquier archivo .rxdb y produce un archivo con todos
# sus microdatos, sin asumir nada sobre la estructura interna.
#
# Uso previsto: bases RedatamX de otros paises o relevamientos distintos
# al Censo 2022. Para Argentina 2022, usar extraer_redatam().
#
# Compatibilidad: solo formato .rxdb (RedatamX).
# No compatible con Redatam7 (.dicx + .rbf), como el Censo Ecuador 2022.
# =============================================================================


#' Extraer microdatos de cualquier base RedatamX (.rxdb)
#'
#' @description
#' Convierte una base de datos en formato RedatamX (.rxdb) a un archivo
#' de salida con todos sus microdatos, sin asumir ninguna estructura
#' predefinida de variables, entidades ni jerarquia geografica.
#'
#' La funcion detecta automaticamente la entidad mas granular de la
#' jerarquia (entidad hoja), construye bloques de variables dinamicamente
#' desde el diccionario, y extrae cada bloque en un subproceso
#' independiente para una gestion eficiente de la memoria RAM.
#'
#' Para el Censo Argentina 2022, utilice \code{\link{extraer_redatam}},
#' que incluye filtrado provincial, reconstruccion de identificadores
#' jerarquicos y verificacion de integridad.
#'
#' @param dic_path Character. Ruta al archivo de diccionario (.rxdb).
#' @param output_file Character. Ruta al archivo de salida. Si es
#'   \code{NULL} (default), se genera automaticamente en el mismo
#'   directorio que \code{dic_path} con el nombre de la base.
#' @param max_por_bloque Integer. Numero maximo de variables por bloque
#'   de extraccion. Default 10. Reducir en equipos con poca memoria RAM.
#' @param formato Character. Formato de salida: \code{"parquet"} (default)
#'   o \code{"csv"}.
#' @param verbose Logico. Si \code{TRUE} (default), muestra el progreso
#'   detallado en consola.
#'
#' @details
#' ## Requisito previo
#'
#' Requiere el motor RedatamX con el limite de extraccion ampliado.
#' Verifique el estado del motor con \code{censo_verificar_engine()}
#' antes de usar esta funcion.
#'
#' ## Compatibilidad
#'
#' Compatible con bases en formato \strong{.rxdb} (RedatamX). No es
#' compatible con el formato Redatam7 (.dicx + .rbf), utilizado por
#' censos como el de Ecuador 2022.
#'
#' ## Gestion de memoria
#'
#' La extraccion se realiza en bloques de \code{max_por_bloque} variables.
#' Cada bloque se procesa en un subproceso Rscript independiente que
#' libera la memoria del motor C++ al finalizar. El proceso principal
#' solo une los bloques leidos desde disco.
#'
#' ## Paises con bases RedatamX conocidas (ronda 2020)
#'
#' \itemize{
#'   \item \strong{Argentina 2022}: usar \code{extraer_redatam()} para
#'     el pipeline completo.
#'   \item \strong{Guatemala, Mexico, Bolivia}: compatibilidad pendiente
#'     de verificacion.
#' }
#'
#' @return Invisible. La ruta al archivo generado.
#'
#' @examples
#' \dontrun{
#' # Uso basico
#' extraer_rxdb(
#'   dic_path    = "/ruta/a/base.rxdb",
#'   output_file = "/ruta/salida/microdatos.parquet"
#' )
#'
#' # Con bloques mas pequenos para equipos con poca memoria RAM
#' extraer_rxdb(
#'   dic_path       = "/ruta/a/base.rxdb",
#'   output_file    = "/ruta/salida/microdatos.parquet",
#'   max_por_bloque = 5
#' )
#' }
#'
#' @seealso
#' \code{\link{extraer_redatam}} para el Censo Argentina 2022,
#' \code{\link{censo_verificar_engine}} para verificar el motor.
#'
#' @export
extraer_rxdb <- function(
    dic_path,
    output_file    = NULL,
    max_por_bloque = 10,
    formato        = "parquet",
    verbose        = TRUE
) {

  if (!file.exists(dic_path))
    stop("Archivo no encontrado: ", dic_path)

  formato  <- match.arg(formato, c("parquet", "csv"))
  base_nom <- tools::file_path_sans_ext(basename(dic_path))

  # Resolver ruta de salida
  if (is.null(output_file)) {
    ext         <- if (formato == "parquet") ".parquet" else ".csv"
    output_file <- file.path(dirname(dic_path), paste0(base_nom, ext))
  }

  # Preparar directorio temporal y archivo de log
  log_file <- file.path(
    dirname(output_file),
    paste0(base_nom, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log")
  )
  tmp_dir <- file.path(dirname(output_file), paste0(".tmp_", base_nom))
  dir.create(tmp_dir,           recursive = TRUE, showWarnings = FALSE)
  dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)

  # Funcion de log interna: escribe en consola y en archivo
  .log <- function(msg, nivel = "INFO") {
    linea <- paste0("[", format(Sys.time(), "%H:%M:%S"), "] [", nivel, "] ", msg)
    if (verbose || nivel %in% c("WARN", "ERROR")) cat(linea, "\n")
    cat(linea, "\n", file = log_file, append = TRUE)
  }

  .log("=== INICIO EXTRACCION REDATAM GENERICA ===")
  .log(paste("Diccionario:", dic_path))
  .log(paste("Output:", output_file))
  .log(paste("Formato:", formato))
  .log(paste("Max vars por bloque:", max_por_bloque))

  # -------------------------------------------------------------------------
  # PASO 1: Leer el diccionario y detectar la entidad hoja
  #
  # La jerarquia de entidades esta ordenada de mayor a menor nivel
  # geografico. La ultima entidad (hoja) es la unidad de analisis
  # mas granular - personas, viviendas, etc. - y define el numero
  # de filas del dataset resultante.
  # -------------------------------------------------------------------------
  .log("PASO 1: Analizando diccionario...")

  dic <- redatam_open(dic_path)

  entidades <- tryCatch(
    redatam_entities(dic)$name,
    error = function(e) stop("No se pudo leer el diccionario: ", conditionMessage(e))
  )

  .log(paste("Jerarquia detectada:", paste(entidades, collapse = " -> ")))

  entidad_hoja <- entidades[length(entidades)]
  .log(paste("Entidad hoja:", entidad_hoja))

  # -------------------------------------------------------------------------
  # PASO 2: Construir bloques de variables
  #
  # Se recopilan todas las variables de todas las entidades. Las variables
  # de entidades superiores llevan el prefijo de su entidad (ej: HOGAR.H01)
  # para que el motor pueda identificarlas en la consulta TABLE VIEW.
  #
  # Las dos primeras variables de la entidad hoja se usan como variables
  # de control para verificar coherencia entre bloques.
  # -------------------------------------------------------------------------
  .log("PASO 2: Construyendo bloques de variables...")

  todas_vars <- character(0)
  for (ent in entidades) {
    vars_ent <- tryCatch(
      redatam_variables(dic, ent)$name,
      error = function(e) character(0)
    )
    if (length(vars_ent) == 0) next
    if (ent == entidad_hoja) {
      # Variables de la entidad hoja: sin prefijo en la consulta SPC
      todas_vars <- c(todas_vars, vars_ent)
    } else {
      # Variables de entidades superiores: requieren prefijo ENTIDAD.
      todas_vars <- c(todas_vars, paste0(ent, ".", vars_ent))
    }
  }
  todas_vars <- unique(todas_vars)
  .log(paste("Total variables en diccionario:", length(todas_vars)))

  # Variables de control: presentes en todos los bloques para validacion cruzada
  vars_hoja    <- tryCatch(redatam_variables(dic, entidad_hoja)$name,
                           error = function(e) character(0))
  vars_control <- vars_hoja[seq_len(min(2, length(vars_hoja)))]
  .log(paste("Variables de control:", paste(vars_control, collapse = ", ")))

  vars_sustantivas <- setdiff(todas_vars, vars_control)

  # Dividir en bloques de max_por_bloque variables
  n       <- length(vars_sustantivas)
  indices <- split(seq_len(n), ceiling(seq_len(n) / max_por_bloque))
  bloques <- lapply(indices, function(idx) c(vars_control, vars_sustantivas[idx]))
  names(bloques) <- paste0("b", seq_along(bloques))

  .log(paste("Total bloques:", length(bloques)))

  # CRITICO: cerrar el diccionario antes de lanzar subprocesos.
  # Un diccionario abierto en la sesion principal bloquea su apertura
  # en los subprocesos hijos, causando un error de acceso al archivo.
  redatam_close(dic)
  rm(dic); gc()
  .log("Diccionario cerrado. Iniciando subprocesos...")

  # -------------------------------------------------------------------------
  # PASO 3: Extraer bloques en subprocesos independientes
  #
  # Cada bloque se extrae en un proceso Rscript separado. Al terminar,
  # el sistema operativo libera la memoria acumulada por el motor C++,
  # evitando que la sesion principal se sature de RAM.
  #
  # Si un bloque ya existe en disco (extraccion anterior interrumpida),
  # se salta para permitir reanudar sin repetir trabajo.
  # -------------------------------------------------------------------------
  .log("PASO 3: Extrayendo bloques...")

  for (k in seq_along(bloques)) {
    nom    <- names(bloques)[k]
    vars_b <- bloques[[k]]
    spc    <- paste("TABLE VIEW", entidad_hoja,
                    paste(vars_b, collapse = ", "))
    out_b  <- file.path(tmp_dir, paste0(nom, ".rds"))

    cat(sprintf("[%-50s] %d/%d (%d%%) %s\n",
                paste(rep("=", round(k / length(bloques) * 50)), collapse = ""),
                k, length(bloques),
                round(k / length(bloques) * 100), nom))

    .log(paste("Extrayendo", nom, "/", length(bloques)))

    # Reanudar extraccion si el bloque ya fue procesado
    if (file.exists(out_b)) {
      .log(paste("  Bloque", nom, "ya existe, saltando"))
      next
    }

    tmp_script <- file.path(tmp_dir, paste0(nom, ".R"))
    script <- sprintf('
suppressMessages(library(redatamx))
tryCatch({
  dic <- redatam_open("%s")
  df  <- as.data.frame(redatam_query(dic, "%s")[[1]])
  redatam_close(dic)
  saveRDS(df, "%s")
  cat("OK", nrow(df), "filas\\n")
}, error = function(e) {
  cat("ERROR:", conditionMessage(e), "\\n")
  quit(status = 1)
})
', dic_path, spc, out_b)

    writeLines(script, tmp_script)
    output <- system(paste("Rscript", shQuote(tmp_script), "2>&1"),
                     intern = TRUE, wait = TRUE)
    ret <- attr(output, "status")
    ret <- if (is.null(ret)) 0L else ret

    for (linea in output) .log(paste(" [Rscript]", linea))
    file.remove(tmp_script)

    if (ret != 0 || !file.exists(out_b)) {
      .log(paste("FALLO bloque", nom), "ERROR")
      unlink(tmp_dir, recursive = TRUE)
      stop("Extraccion fallida en bloque ", nom, ". Ver log: ", log_file)
    }

    sz <- round(file.size(out_b) / 1024 / 1024, 1)
    .log(paste("  OK", nom, "-", sz, "MB en disco"))
  }

  # -------------------------------------------------------------------------
  # PASO 4: Unir bloques
  #
  # Los bloques se leen uno a uno desde disco y se unen con cbind.
  # Antes de unir cada bloque nuevo, se verifica que tenga el mismo numero
  # de filas que el bloque base - garantia de que el motor devolvio los
  # registros en el mismo orden en todas las extracciones.
  # -------------------------------------------------------------------------
  .log("PASO 4: Uniendo bloques...")

  df <- NULL
  for (k in seq_along(bloques)) {
    nom    <- names(bloques)[k]
    out_b  <- file.path(tmp_dir, paste0(nom, ".rds"))
    bloque <- readRDS(out_b)

    if (is.null(df)) {
      df <- bloque
      .log(paste("  Bloque", nom, "base -", nrow(df), "filas"))
    } else {
      if (nrow(bloque) != nrow(df)) {
        .log(paste("ERROR: bloque", nom, "tiene", nrow(bloque),
                   "filas vs", nrow(df), "esperadas"), "ERROR")
        unlink(tmp_dir, recursive = TRUE)
        stop("Inconsistencia de filas en bloque ", nom,
             ". Ver log: ", log_file)
      }
      # Excluir variables de control que ya estan en df
      cols_nuevas <- setdiff(names(bloque), vars_control)
      df <- cbind(df, bloque[, cols_nuevas, drop = FALSE])
      .log(paste("  Bloque", nom, "unido"))
    }
    rm(bloque); gc()
  }

  .log(paste("Dataset completo:",
             format(nrow(df), big.mark = ","), "filas x",
             ncol(df), "columnas"))

  # -------------------------------------------------------------------------
  # PASO 5: Guardar y limpiar
  # -------------------------------------------------------------------------
  .log("PASO 5: Guardando...")

  tryCatch({
    if (formato == "parquet") {
      arrow::write_parquet(df, output_file, compression = "snappy")
    } else {
      data.table::fwrite(df, output_file)
    }
    sz <- round(file.size(output_file) / 1024 / 1024, 1)
    .log(paste("Guardado:", basename(output_file), "(", sz, "MB)"))
  }, error = function(e) {
    .log(paste("ERROR guardando:", conditionMessage(e)), "ERROR")
    stop(conditionMessage(e))
  })

  # Eliminar archivos temporales
  unlink(tmp_dir, recursive = TRUE)
  .log("Bloques temporales eliminados")
  .log("=== EXTRACCION COMPLETADA ===")

  invisible(output_file)
}
