#!/usr/bin/env bash
set -e

/code/docker/wait-for-it.sh --strict --timeout=120 internal-api:80 mysql:3306
composer ci
