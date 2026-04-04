# censo2022arg

<!-- badges: start -->
[![R-CMD-check](https://github.com/RodriDuran/censo2022arg/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/RodriDuran/censo2022arg/actions/workflows/R-CMD-check.yaml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19422784.svg)](https://doi.org/10.5281/zenodo.19422784)
<!-- badges: end -->

**censo2022arg** permite extraer, etiquetar y leer los microdatos del Censo
Nacional de Poblacion, Hogares y Viviendas 2022 de Argentina desde las bases
REDATAM distribuidas oficialmente por el
[INDEC](https://www.indec.gob.ar).

## Caracteristicas principales

- Extraccion completa de microdatos provincia por provincia
- Reconstruccion de identificadores jerarquicos (vivienda, hogar, persona)
- Etiquetado automatico de variables desde los diccionarios oficiales del INDEC
- Verificacion de integridad contra los totales publicados por el INDEC
- Gestion eficiente de memoria mediante subprocesos independientes
- Salida en formato Parquet (default), CSV, SPSS o SAS
- Compatible con cualquier base RedatamX (.rxdb)

## Instalacion
Este paquete se apoya en [`redatamx`](https://github.com/ideasybits/redatamx4r)
de Jaime Salvador para la comunicacion con el motor REDATAM desarrollado por
CELADE (CEPAL). Ambos se instalan automaticamente como dependencias.
```r
# Version de desarrollo desde GitHub
# install.packages("remotes")
remotes::install_github("RodriDuran/censo2022arg")
```

## Uso basico

```r
library(censo2022arg)

# Ver el estado actual de la configuracion
censo_info()

# 1. Configurar el directorio de datos (solo la primera vez)
censo_configurar("/ruta/a/mis/datos/censo2022", persistent = TRUE)

# 2. Verificar el motor de extraccion y seguir las instrucciones
censo_verificar_engine()

# 3. Descargar las bases desde el INDEC (~500 MB)
censo_descargar()

# 4. Extraer los microdatos
extraer_redatam()                            # todas las provincias
extraer_redatam(provincias = 66)             # solo Salta (prueba rapida)
extraer_redatam(provincias = c(66, 38, 34)) # varias provincias

# 5. Etiquetar las variables con los diccionarios oficiales
censo_etiquetar()

# 6. Leer y analizar los datos

# Personas de Salta
personas <- censo_leer(base = "Personas", provincias = 66)

# Hogares de Salta y Jujuy, solo algunas variables
hogares <- censo_leer(
  base       = "Hogares",
  provincias = c(66, 38),
  columnas   = c("NBI_1", "NBI_2", "TIPHOGAR")
)

# Personas mayores de 18 con filtro aplicado antes de cargar en RAM
mayores <- censo_leer(
  base       = "Personas",
  provincias = 66,
  columnas   = c("EDAD", "CONDACT", "IDRADIO"),
  filtro     = quote(EDAD >= 18)
)

# Hogares de todo el pais como data.table
hogares_arg <- censo_leer(base = "Hogares", formato = "data.table")

# Extraer microdatos de cualquier base RedatamX generica
extraer_rxdb(dic_path = "/ruta/a/base.rxdb")
```

## Bases disponibles

El INDEC distribuye tres bases complementarias del Censo 2022:

| Base | Archivo | Contenido |
|------|---------|-----------|
| VP | `cpv2022.rxdb` | Viviendas particulares -- variables de persona, hogar y vivienda |
| PO | `cpv2022.rxdb` | Pueblos originarios, afrodescendientes e identidad de genero |
| VC | `cpv2022col.rxdb` | Viviendas colectivas |

El pipeline combina VP y PO automaticamente, obteniendo el radio censal de VP
y las variables adicionales de PO.

## Nota sobre los datos

Este paquete **no distribuye datos del censo**. Los datos deben descargarse
directamente desde el portal oficial del INDEC:
<https://www.indec.gob.ar/indec/web/Institucional-Indec-BasesDeDatos>

Los datos del Censo 2022 estan protegidos por la Ley N 17.622 de secreto
estadistico. Su uso esta permitido exclusivamente con fines estadisticos
y de investigacion.

## Citacion

Si utilizas este paquete en tu investigacion, por favor citalo:

```
Duran, R. J. (2026). censo2022arg: Extraccion y Procesamiento de Microdatos
del Censo Nacional 2022 de Argentina. R package version 1.0.0.
https://github.com/RodriDuran/censo2022arg
```

## Licencia

GPL (>= 3). Ver [LICENSE](https://github.com/RodriDuran/censo2022arg/blob/main/LICENSE) para mas detalles.
