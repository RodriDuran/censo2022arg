# censo2022arg

<!-- badges: start -->
[![CRAN status](https://www.r-pkg.org/badges/version/censo2022arg)](https://cran.r-project.org/package=censo2022arg)
[![CRAN downloads total](https://cranlogs.r-pkg.org/badges/grand-total/censo2022arg)](https://cran.r-project.org/package=censo2022arg)
[![R-CMD-check](https://github.com/RodriDuran/censo2022arg/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/RodriDuran/censo2022arg/actions/workflows/R-CMD-check.yaml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
<!-- badges: end -->

<p align="center">
  <img src="man/figures/logocenso2022arg.png" alt="censo2022arg logo" width="150"/>
</p>

**censo2022arg** permite extraer, etiquetar y leer los microdatos del Censo
Nacional de Población, Hogares y Viviendas 2022 de Argentina desde las bases
'REDATAM' distribuidas oficialmente por el
[INDEC](https://www.indec.gob.ar).

## Características principales

- Extracción completa de microdatos provincia por provincia
- Reconstrucción de identificadores jerárquicos (vivienda, hogar, persona)
- Etiquetado automático de variables desde los diccionarios oficiales del INDEC
- Verificación de integridad contra los totales publicados por el INDEC
- Gestión eficiente de memoria mediante subprocesos independientes
- Salida en formato 'Parquet' (default), 'CSV', 'SPSS' o 'SAS'
- Compatible con cualquier base 'RedatamX' (.rxdb)

## Instalación

```r
# Desde CRAN (recomendado)
install.packages("censo2022arg")

# Versión de desarrollo desde GitHub
# install.packages("remotes")
remotes::install_github("RodriDuran/censo2022arg")
```

Este paquete se apoya en [`redatamx`](https://github.com/ideasybits/redatamx4r)
de Jaime Salvador para la comunicación con el motor 'REDATAM' desarrollado por
CELADE (CEPAL). Ambas dependencias se instalan automáticamente.

## Uso básico

La función `censo_info()` es el punto de entrada recomendado, diagnostica el
estado actual del proceso de extracción y análisis, guiando al usuario paso a
paso. Detecta automáticamente qué etapa del flujo ya fue completada y cuál debe
ejecutarse a continuación. Se recomienda ejecutarla después de cada paso; su
diagnóstico se actualizará para reflejar el progreso alcanzado.

```r
library(censo2022arg)

# Punto de entrada recomendado: diagnostica el estado e indica el próximo paso
censo_info()

# 1. Configurar el directorio de datos (solo la primera vez)
censo_configurar("/ruta/a/mis/datos/censo2022", persistent = TRUE)

# 2. Verificar el motor de extracción y seguir las instrucciones
censo_verificar_engine()

# 3. Descargar las bases desde el INDEC (~500 MB)
censo_descargar()

# 4. Extraer los microdatos
extraer_redatam()                            # todas las provincias
extraer_redatam(provincias = 66)             # solo Salta (prueba rápida)
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

# Hogares de todo el país como data.table
hogares_arg <- censo_leer(base = "Hogares", formato = "data.table")

# Extraer microdatos de cualquier base RedatamX genérica
extraer_rxdb(dic_path = "/ruta/a/base.rxdb")
```

## Bases disponibles

El INDEC distribuye tres bases complementarias del Censo 2022:

| Base | Archivo | Contenido |
|------|---------|-----------|
| 'VP' | `cpv2022.rxdb` | Viviendas particulares — variables de persona, hogar y vivienda |
| 'PO' | `cpv2022.rxdb` | Pueblos originarios, afrodescendientes e identidad de género |
| 'VC' | `cpv2022col.rxdb` | Viviendas colectivas |

El pipeline combina 'VP' y 'PO' automáticamente, obteniendo el radio censal de 'VP'
y las variables adicionales de 'PO' (P03, P22, P23, P24, P25, IDETNICA).

## Nota sobre los datos

Este paquete **no distribuye datos del censo**. Los datos deben descargarse
directamente desde el portal oficial del INDEC:
<https://www.indec.gob.ar/indec/web/Institucional-Indec-BasesDeDatos>

Los datos del Censo 2022 están protegidos por la Ley N° 17.622 de secreto
estadístico. Su uso está permitido exclusivamente con fines estadísticos
y de investigación.

## Problema conocido en la distribución oficial del INDEC

Las provincias de **Tucumán (cód. 90)** y **Tierra del Fuego (cód. 94)**
presentan conteos incorrectos en las bases distribuidas por el INDEC
(Tucumán devuelve 310.725 personas en lugar de 1.727.337; Tierra del Fuego
devuelve 0). El problema fue confirmado en las tres bases ('VP', 'PO', 'VC') y
**no es un error de este paquete**. Las 22 provincias restantes verifican
con diferencia cero respecto a las tablas de control oficiales.
Reporte enviado a: censo2022@indec.gob.ar

## Citación

Si utilizas este paquete en tu investigación, por favor cítalo:

```
Duran, R. J. (2026). censo2022arg: Extracción y Procesamiento de Microdatos
del Censo Nacional 2022 de Argentina. Versión 1.0.1. R package.
doi:10.32614/CRAN.package.censo2022arg
```

En formato BibTeX:

```bibtex
@software{duran2026censo2022arg,
  author  = {Dur{\'a}n, Rodrigo Javier},
  title   = {{censo2022arg}: Extracci{\'o}n y Procesamiento de
             Microdatos del Censo Nacional 2022 de Argentina},
  year    = {2026},
  version = {1.0.1},
  doi     = {10.32614/CRAN.package.censo2022arg},
  url     = {https://doi.org/10.32614/CRAN.package.censo2022arg}
}
```

## Licencia

GPL (>= 3). Ver [LICENSE](https://www.gnu.org/licenses/gpl-3.0.html) para más detalles.
```
