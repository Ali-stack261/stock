# Real-Time Stock Prediction Platform

This repository contains a starter implementation for the real-time stock price prediction MLOps platform described in the project README.

## Project Structure

- `producer/` – ingestion and event normalization modules
- `tests/` – unit tests for the initial Phase 1 implementation

## Setup

```bash
pip install -r requirements.txt
python -m pytest -q tests/unit/test_phase1.py
```
