#!/usr/bin/env bash
set -e

/code/docker/wait-for-it.sh --strict --timeout=120 internal-api:3000
curl -X POST http://internal-api:3000/index -H 'Content-Type: application/json'
composer ci
