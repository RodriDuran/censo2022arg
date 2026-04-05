## Resubmission

This is a resubmission. Changes based on CRAN feedback:

* Removed non-standard 'DOI' field from DESCRIPTION
* Added DOI citation inline in the Description field following the form
  Authors (year) <doi:10.xxxxx/zenodo.xxxxxxx>

## Package description

This package allows extracting, labeling and reading microdata from the
2022 National Population, Households and Housing Census of Argentina from
the REDATAM databases officially distributed by INDEC (Argentina's National
Institute of Statistics and Census).

The package depends on `redatamx` (available on CRAN), which provides the
R interface to the REDATAM engine developed by CELADE (CEPAL - United
Nations). The REDATAM engine is a closed-source binary freely distributed
by CELADE and included in the `redatamx` package. This package does not
distribute the engine nor modify its source code.

Census data is not distributed with the package. Users download it
directly from the official INDEC portal (https://www.indec.gob.ar).

## R CMD check results

0 errors | 0 warnings | 1 note

* checking compilation flags used: NOTE
  Compilation used the following non-portable flag(s):
  '-Werror=format-security' '-Wp,-D_GLIBCXX_ASSERTIONS'
  '-Wp,-U_FORTIFY_SOURCE,-D_FORTIFY_SOURCE=3' '-march=x86-64'
  '-mno-omit-leaf-frame-pointer' '-mtls-dialect=gnu2'

  These flags are imposed by the Fedora Linux 43 system compiler
  (GCC 15.2.1, Red Hat) and are not set in the package Makevars.
  They do not affect portability or functionality on other platforms.

## Test environments

* Fedora Linux 43, R 4.5.2
* GitHub Actions: ubuntu-latest, R release (via r-lib/actions)
* Windows (win-builder), R 4.6.0 alpha
* macOS: not tested due to R-hub service unavailability at submission time.

## Downstream dependencies

None. This is a resubmission.
