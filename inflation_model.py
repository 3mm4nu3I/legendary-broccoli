import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.stats.diagnostic import acorr_breusch_godfrey
import yfinance as yf
import pandas_datareader.data as web
import datetime
import warnings

warnings.filterwarnings('ignore')

print("Initiating Live Telemetry Pull...")

# Define timeframe (Last 5 years captures the massive 2020-2026 volatility)
end_date = datetime.datetime.now()
start_date = end_date - datetime.timedelta(days=5 * 365)

# ---------------------------------------------------------
# 1. FETCH FRED DATA (Macroeconomic Indicators)
# ---------------------------------------------------------
print("Fetching FRED Data (CPI, M2 Velocity)...")

fred_raw = web.DataReader(['CPIAUCSL', 'M2V'], 'fred', start_date, end_date)

# CPI is monthly already
cpi = fred_raw['CPIAUCSL'].resample('MS').ffill()
I = cpi.pct_change(12).rename('I')

# M2V: compute growth on the native QUARTERLY series first, then spread to
# monthly. Doing pct_change() after ffill() produces a false staircase
# (0, 0, jump) that distorts the M coefficient and clusters "shocks" at
# quarter boundaries, which also messes with HAC standard errors.
m2v_q = fred_raw['M2V'].dropna()
m2v_growth_q = m2v_q.pct_change().rename('M')
M = m2v_growth_q.resample('MS').ffill()

fred_data = pd.concat([I, M], axis=1)

# ---------------------------------------------------------
# 2. FETCH YAHOO FINANCE DATA (Commodities & Transport)
# ---------------------------------------------------------
print("Fetching Yahoo Finance Data (Brent Crude, Shipping Proxy)...")
tickers = ['BZ=F', 'BDRY']
yf_raw = yf.download(tickers, start=start_date, end=end_date, progress=False)

# yfinance's column shape varies by version (MultiIndex vs flat single-ticker
# frame). Normalize explicitly instead of assuming ['Close'] always works.
if isinstance(yf_raw.columns, pd.MultiIndex):
    yf_data = yf_raw['Close']
else:
    yf_data = yf_raw[['Close']].rename(columns={'Close': tickers[0]})

missing = set(tickers) - set(yf_data.columns)
assert not missing, f"Missing tickers in Yahoo data: {missing}"

yf_data = yf_data.resample('MS').mean()
yf_data['E'] = yf_data['BZ=F'].pct_change()
yf_data['T'] = yf_data['BDRY'].pct_change()

# ---------------------------------------------------------
# 3. MERGE, CLEAN, AND SET THRESHOLDS
# ---------------------------------------------------------
df = pd.concat([fred_data[['I', 'M']], yf_data[['E', 'T']]], axis=1).dropna()

# Threshold choice: full-sample percentile vs expanding-window percentile.
# Full-sample uses future data to classify past months as "shocks" (fine for
# a purely descriptive/historical regression, NOT fine if this is meant to
# emulate a live signal). Set USE_EXPANDING_THRESHOLD to pick.
USE_EXPANDING_THRESHOLD = True
MIN_WARMUP = 24  # months of history required before a shock can be flagged

if USE_EXPANDING_THRESHOLD:
    df['barE'] = df['E'].expanding(min_periods=MIN_WARMUP).quantile(0.90)
    df['th'] = (df['E'] > df['barE']).astype(float)
    df.loc[df['barE'].isna(), 'th'] = np.nan
    df = df.dropna(subset=['th'])
    barE_display = df['barE'].iloc[-1]  # most recent threshold, for display only
else:
    barE_display = np.percentile(df['E'], 90)
    df['th'] = (df['E'] > barE_display).astype(float)

# Construct Interaction Vectors
df['E_th'] = df['E'] * df['th']
df['E_T'] = df['E'] * df['T']
df['E_M'] = df['E'] * df['M']
df.to_csv("model_data.csv")

print(f"\n[+] Data compiled successfully. Total monthly observations: {len(df)}")
print(f"[+] Threshold mode: {'expanding-window' if USE_EXPANDING_THRESHOLD else 'full-sample'}")
print(f"[+] Most recent Energy Threshold (barE): {barE_display*100:.2f}% shock boundary.")


# ---------------------------------------------------------
# 4. HELPER: Newey-West lag selection via Breusch-Godfrey
# ---------------------------------------------------------
def select_hac_lags(ols_model, max_lag=12, alpha=0.05):
    """
    Fit an OLS with plain (non-robust) covariance first, run Breusch-Godfrey
    LM tests for serial correlation at increasing lag orders, and return the
    smallest lag at which residual autocorrelation is no longer significant.
    This replaces a hardcoded maxlags=3 with a data-driven choice.
    """
    chosen_lag = 1
    for lag in range(1, max_lag + 1):
        bg_stat, bg_pvalue, _, _ = acorr_breusch_godfrey(ols_model, nlags=lag)
        chosen_lag = lag
        if bg_pvalue > alpha:
            break
    return chosen_lag


# ---------------------------------------------------------
# 5. RUN ADDITIVE OLS ENGINE
# ---------------------------------------------------------
X_add = sm.add_constant(df[['E', 'E_th', 'T', 'M', 'E_T', 'E_M']])

# Plain OLS fit first, purely to run the BG test and pick a lag length
plain_add = sm.OLS(df['I'], X_add).fit()
lags_add = select_hac_lags(plain_add)
print(f"\n[+] Selected HAC maxlags for additive model: {lags_add}")

model_add = sm.OLS(df['I'], X_add).fit(cov_type='HAC', cov_kwds={'maxlags': lags_add})
print("\n=== LIVE ADDITIVE MODEL EXECUTION ===")
print(model_add.summary().tables[1])

# ---------------------------------------------------------
# 6. RUN MULTIPLICATIVE LOG ENGINE
# ---------------------------------------------------------
# NOTE: E, T, M are already small monthly returns, so log1p(x) ~= x for most
# observations -- this specification will look very close to the additive
# model numerically and isn't a true elasticity model. Kept here per request,
# but a genuine multiplicative/elasticity model would use log-LEVELS of the
# underlying series (log CPI index, log Brent price, log M2V level) instead.
df['ln1I'] = np.log1p(df['I'])
df['ln1E'] = np.log1p(df['E'])
df['ln1T'] = np.log1p(df['T'])
df['ln1M'] = np.log1p(df['M'])

df['lnE_lnT'] = df['ln1E'] * df['ln1T']
df_ln1E_th = (df['ln1E'] * df['th']).rename('ln1E_th')

X_mul = sm.add_constant(pd.concat(
    [df['ln1E'], df_ln1E_th, df['ln1T'], df['ln1M'], df['lnE_lnT']], axis=1
))

plain_mul = sm.OLS(df['ln1I'], X_mul).fit()
lags_mul = select_hac_lags(plain_mul)
print(f"\n[+] Selected HAC maxlags for multiplicative model: {lags_mul}")

model_mul = sm.OLS(df['ln1I'], X_mul).fit(cov_type='HAC', cov_kwds={'maxlags': lags_mul})
print("\n=== LIVE MULTIPLICATIVE LOG MODEL EXECUTION ===")
print(model_mul.summary().tables[1])
