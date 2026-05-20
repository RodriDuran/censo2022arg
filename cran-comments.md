## Resubmission (v1.1.0)

This is a new submission with the following changes:

### New features

**1. censo_descomprimir() — new function**
Added a new exported function to decompress and organize the census ZIP files
into the internal package directory structure. Handles CP850 encoding in ZIP
filenames (present in INDEC's official distribution) via the 'archive' package.
Automatically detects each base (VP, PO, VC) by folder name (with synonyms)
and by file content. If a folder cannot be identified, it is copied to
censo_dir_bases() with its original name and the user is instructed to rename
it manually.

**2. censo_descargar() — improved error detection**
The function now saves the ZIP file locally instead of extracting immediately,
allowing censo_descomprimir() to process it separately. Added validation to
detect when INDEC's server returns an HTML error page instead of the ZIP file
(as has occurred since May 4, 2026, when INDEC suspended local distribution
of census bases). In this case, the function informs the user and provides
contact information to request access.

**3. New dependency: 'archive'**
Added 'archive' to Imports. This package provides robust ZIP extraction
handling CP850-encoded filenames that cannot be processed by base R's unzip().

### Bug fixes

**4. censo_descargar() — metadatos block**
The metadatos download block now follows the same validation pattern as the
bases block: saves ZIP locally, validates it is a real ZIP file before
processing, and provides informative error messages if INDEC's server is
unavailable.

**5. Updated user guidance**
Updated .censo_bienvenida(), censo_info(), and censo_etiquetar() messages
to reflect the new two-step workflow (censo_descargar() + censo_descomprimir())
and to inform users about the alternative etiquetado source
(fuente_meta = 'redatam') when XLS metadata files are unavailable.

## R CMD check results

0 errors | 0 warnings | 1 note

* checking for future file timestamps: NOTE
  Unable to verify current time. This is a connectivity issue on the
  check server and is unrelated to the package.

## Test environments

* Fedora Linux 43, R 4.5.2
* Windows 10, R 4.5.2
* GitHub Actions: ubuntu-latest, R release (via r-lib/actions)

## Downstream dependencies

None.
