# dla-opac-ezb

Transformation der Daten aus der Elektronischen Zeitschriftenbibliothek (EZB) für den Katalog des Deutschen Literaturarchivs Marbach

## Voraussetzungen

- GNU/Linux mit GNU awk und Perl
- [go-task](https://taskfile.dev) >=3.0.0
- Python 3.11+ mit den Paketen aus `requirements.txt`

Alles Nötige enthält der Devcontainer (`.devcontainer/`). Der Ablauf für Änderungen, Datenaktualisierung und Release steht in der [CONTRIBUTING.md](CONTRIBUTING.md).

## Input

* Datenabzug im Format KBART erstellen (bereits mit Filter auf für das DLA einschläge Zeitschriften)
* Bereitstellung der Datei in `input/ezb-dla-kbart.tsv`

## Nutzung

```sh
task
```
