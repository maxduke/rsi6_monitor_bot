#!/bin/bash
docker run --rm -v "$(pwd):/app" -w /app python:3.12-slim-bookworm sh -c "pip install pip-tools && pip-compile requirements.in -o requirements.txt"
