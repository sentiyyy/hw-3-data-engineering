#!/usr/bin/env sh
set -eu
if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env; review local credentials before continuing."
fi
docker compose up -d rustfs
docker compose run --rm pipeline all --limit 1200 --chunk-sizes 100,500,1000 --attempts 3
