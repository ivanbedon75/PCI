# Systematic Review Pipeline

Pipeline auditable y reproducible para revisiones sistemáticas multifuente con soporte para:

- OpenAlex
- Scopus CSV


## Objetivos

Este proyecto fue diseñado para facilitar la reproducibilidad exigida por revisores metodológicos. 

Por ello:

- separa captura, armonización, deduplicación y exportación;
- genera manifiesto con hashes SHA-256;
- ordena registros de forma determinista;
- exporta artefactos intermedios y finales;
- produce conteos PRISMA automáticos;
- crea matriz de screening con doble revisor;
- permite validación configurable por protocolo.

## Garantías de auditabilidad

Este proyecto fue diseñado para revisión metodológica exigente. Cada corrida produce:

- `audit_trail.jsonl`: eventos por etapa, en orden temporal.
- `run_log.jsonl`: log técnico estructurado.
- `manifest.json`: hashes SHA-256 y tamaños de archivos.
- `raw_records.json`: registros armonizados antes de deduplicación.
- `deduplicated_records.csv`: corpus consolidado final.
- `quality_profile.json`: perfil cuantitativo de metadatos.
- `prisma_counts.json`: conteos PRISMA automáticos.
- `screening_matrix.csv`: matriz para doble revisor.


## Principios de reproducibilidad para GitHub

Para que un revisor obtenga los mismos resultados:

1. fija una release o tag del repositorio;
2. conserva el protocolo JSON usado;
3. conserva las exportaciones CSV de Scopus/WoS;
4. si usas APIs vivas, guarda snapshots crudos del día de captura;
5. ejecuta el pipeline con las mismas entradas y la misma versión del código;
6. compara `manifest.json` y `audit_trail.jsonl`.

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .

## Uso rápido
```bash
sra-tool run \
  --source scopus_csv --file data/external/scopus.csv \
  --protocol config/protocol.example.json \
  --output-dir outputs/run_local

sra-tool run \
  --source openalex --query "systematic review reproducibility" \
  --source scopus_csv --file data/external/scopus.csv \
  --protocol config/protocol.example.json \
  --output-dir outputs/run_mixed


