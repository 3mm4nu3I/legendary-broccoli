# legendary-broccoli
Macro-economic analyst, Fraud Prevention and Cyber-security Tech Expert
# Macroeconomic Transmission & Attrition Engine

## Overview
An institutional-grade econometric framework designed to track non-linear inflation transmission vectors, energy shocks, and monetary friction. Built to eliminate look-ahead bias and address structural data vulnerabilities in macroeconomic forecasting.

## Key Technical Features
- **Expanding-Window Thresholds:** Replaces full-sample percentiles with an expanding 90th-percentile window (with a 24-month warmup) to completely eliminate look-ahead bias in live signaling.
- **Dynamic HAC Lag Selection:** Integrates automated Breusch-Godfrey LM testing (lag orders 1–12) to dynamically determine Newey-West standard error truncations, ensuring robust inference under serial correlation.
- **Velocity Interpolation & Alignment:** Corrects for quarterly-to-monthly M2 velocity step distortions and aligns time-series horizons.
- **Dual-Engine Estimation:** Evaluates shocks through both an additive OLS regression and a multiplicative log-space robustness check (`log1p`).

## Repository Contents
- `inflation_model.py`: The core telemetry and regression execution script.
- `model_data.csv`: Sample monthly observation dataset (2024–2026).
- `docs/architecture_of_accountability.pdf`: Comprehensive white paper detailing the econometric methodology, model limitations, and operational context.

## Author
BEAN
