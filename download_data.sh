#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# crunch — téléchargement des pageviews horaires Wikimédia (sujet §2)
#
#   ./scripts/download_data.sh                 # 7 jours à partir du 2026-06-01
#   ./scripts/download_data.sh 2026-06 2026-06-08 7
#   ./scripts/download_data.sh --verify 2026-06
#
# 168 fichiers = 7 jours x 24 heures, ~5 Go compressés.
# Le script est ré-entrant : un fichier déjà présent et gzip-valide est
# ignoré, on peut donc relancer après une coupure.
# ---------------------------------------------------------------------------
set -euo pipefail

BASE="https://dumps.wikimedia.org/other/pageviews"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PARALLEL="${CRUNCH_PARALLEL:-4}"      # 4 connexions : poli pour les miroirs WMF

VERIFY_ONLY=0
if [[ "${1:-}" == "--verify" ]]; then VERIFY_ONLY=1; shift; fi

MONTH="${1:-2026-06}"
START="${2:-${MONTH}-01}"
DAYS="${3:-7}"
DEST="${ROOT}/data/raw/${MONTH}"

mkdir -p "$DEST"

# ---------------------------------------------------------------- vérif ----
verify() {
  local n_ok=0 n_bad=0 f
  shopt -s nullglob
  for f in "$DEST"/*.gz; do
    if gzip -t "$f" 2>/dev/null; then n_ok=$((n_ok + 1)); else
      echo "CORROMPU : $(basename "$f")"; n_bad=$((n_bad + 1)); fi
  done
  local size
  size=$(du -sh "$DEST" 2>/dev/null | cut -f1)
  echo "----------------------------------------"
  echo "dossier : $DEST"
  echo "valides : $n_ok   corrompus : $n_bad   attendu : $((DAYS * 24))"
  echo "taille  : ${size:-0}"
  [[ $n_bad -eq 0 && $n_ok -eq $((DAYS * 24)) ]]
}

if [[ $VERIFY_ONLY -eq 1 ]]; then
  verify && echo "=> jeu de données complet" || { echo "=> incomplet"; exit 1; }
  exit 0
fi

# ------------------------------------------------------- liste des URLs ----
LIST="$(mktemp)"
trap 'rm -f "$LIST"' EXIT

for ((d = 0; d < DAYS; d++)); do
  day=$(date -u -d "${START} +${d} day" +%Y%m%d)
  ym=$(date -u -d "${START} +${d} day" +%Y-%m)
  y=${ym%%-*}
  for h in $(seq -w 0 23); do
    f="pageviews-${day}-${h}0000.gz"
    # déjà là et valide -> on saute
    if [[ -s "${DEST}/${f}" ]] && gzip -t "${DEST}/${f}" 2>/dev/null; then
      continue
    fi
    echo "${BASE}/${y}/${ym}/${f}" >>"$LIST"
  done
done

TOTAL=$(wc -l <"$LIST" | tr -d ' ')
echo "[download] ${TOTAL} fichier(s) à récupérer vers ${DEST}"
[[ "$TOTAL" -eq 0 ]] && { verify; exit 0; }

# ------------------------------------------------------- téléchargement ----
# --retry 5 : les miroirs WMF renvoient parfois des 503 passagers.
xargs -P "$PARALLEL" -I{} curl \
  --location --fail --silent --show-error \
  --retry 5 --retry-delay 3 --retry-connrefused \
  --output-dir "$DEST" --remote-name \
  --user-agent "crunch-ipssi-m2 (usage pedagogique)" \
  "{}" <"$LIST"

echo "[download] terminé, vérification…"
verify && echo "=> jeu de données complet" || { echo "=> relancez le script"; exit 1; }
