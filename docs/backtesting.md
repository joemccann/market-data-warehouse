# Quant Backtesting Framework in Python

Repo note:
- All backtesting and strategy code has been extracted to the standalone **doob** package at `~/dev/apps/finance/doob`.
- The `strategies/` directory in this repo is legacy and no longer the canonical source.
- Use `python -m doob run <strategy>` from the doob package. See the [doob README](~/dev/apps/finance/doob/README.md) for usage.

Prompt:

Role: Lead Quantitative Software Architect
Context: I am building a modular, institutional-grade Python backtesting framework. The system must be "Strategy Agnostic," allowing for the seamless integration of diverse alpha models while maintaining a rigorous "Gold Standard" for performance attribution and data handling.

Task:
Engineer a production-ready Backtesting System based on the synthesized best practices of Yves Hilpisch (High-Performance Data) and Chris Kelliher (Quantitative Foundations).

System Architecture (Modular Requirements):
1. Abstract Data Layer (Hilpisch #31, #34): Use an Interface/Base Class for data ingestion. The default implementation should leverage DuckDB and Parquet for high-speed I/O, but must be extensible to HDF5 or SQL.
2. Signal Engine (Kelliher #56): Implement a strictly vectorized Signal Generator. All signals MUST be timestamp-aligned with a mandatory `.shift(1)` to ensure zero look-ahead bias as a system invariant.
3. Accounting & State Engine: Track 'Sub-Portfolios' including Cash, Margin, and Mark-to-Market (MTM) equity. Handle 'Fully Deployed' vs. 'Fractional' sizing through a dedicated PositionManager.
4. Transaction Cost Analysis (TCA) Module: A configurable fee engine that supports Tiered, Fixed, and Percentage-based commissions, including slippage modeling and regulatory fees (SEC/FINRA).

Quantitative & Risk Guardrails (Kelliher #54, #67):
- Mandatory Metrics: CAGR, Volatility, Sharpe, Sortino, Calmar, and 95%/99% VaR (Parametric & Historical).
- Statistical Validation: Include automated ADF (Stationarity) tests on returns and OLS Regression for Factor Attribution (Beta to Benchmarks).
- Regime Intelligence: Implement an optional 'Regime Filter' base class (e.g., VIX-based or HMM-based) that acts as a global 'circuit breaker' for strategies.

Best Practices to Inject (Top 50 Synthesis):
- Performance: Use @numba.jit for any necessary iterative loops and multiprocessing for walk-forward optimizations (Hilpisch #35, #39).
- Design: Follow Object-Oriented Programming (OOP) with clear 'Valuation' and 'Strategy' class hierarchies (Hilpisch #65).
- Stability: Use Log Returns for all internal compounding math to ensure time-additivity (Hilpisch #15).

Output:
Provide the core boilerplate for this framework, including the Base Classes, a sample 'Strategy' implementation, and a 'PerformanceDashboard' class for visualization.

Reasoning: Think step-by-step. First, define the system's 'Data Flow' (Input -> Signal -> Trade -> Metrics) before writing the code.

In addtion, apply these **25 key takeaways** from *Quantitative Finance with Python* by Chris Kelliher, with brief code samples (using common libraries like numpy, pandas, scipy):

1. Quant landscape includes sell-side, buy-side, fintech; projects involve data → cleaning → modeling → validation.
   
   ```python
   import pandas as pd
   data = pd.read_csv('prices.csv')  # data collection example
   ```
2. Risk-neutral pricing relies on no-arbitrage; uses risk-neutral probabilities.
   
   ```python
   import numpy as np
   def rn_prob(u, d, r): return (np.exp(r) - d) / (u - d)
   ```
3. Binomial tree for option pricing converges to Black-Scholes.
   
   ```python
   def binomial_call(S, K, r, sigma, T, n):
       dt = T / n; u = np.exp(sigma * np.sqrt(dt))
       d = 1/u; p = (np.exp(r*dt) - d)/(u-d)
       # ... build tree
   ```
4. Brownian motion is core to continuous stochastic processes.
   
   ```python
   def brownian_motion(T, n): 
       dt = T/n; return np.cumsum(np.random.randn(n) * np.sqrt(dt))
   ```
5. Itô’s lemma for SDEs: d(f) = … (drift + diffusion terms).
   
   ```python
   # Example: apply to log(S) in GBM → derive BS PDE
   ```
6. Black-Scholes PDE from Feynman-Kac and risk-neutral SDE.
   
   ```python
   from scipy.stats import norm
   def bs_call(S, K, T, r, sigma):
       d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
       return S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d1 - sigma*np.sqrt(T))
   ```
7. Physical measure focuses on forecasting, risk premia, regression.
   
   ```python
   import statsmodels.api as sm
   model = sm.OLS(returns['asset'], sm.add_constant(returns['market'])).fit()
   ```
8. Linear regression estimates betas; watch assumptions (no multicollinearity).
   
   ```python
   beta = np.cov(asset_ret, mkt_ret)[0,1] / np.var(mkt_ret)
   ```
9. Time series: stationarity via differencing, ARMA models.
   
   ```python
   from statsmodels.tsa.arima.model import ARIMA
   model = ARIMA(prices, order=(1,1,1)).fit()
   ```
10. Portfolio diversification reduces risk via low correlations.
    
    ```python
    port_vol = np.sqrt(w.T @ cov @ w)  # w = weights, cov = covariance matrix
    ```
11. Numpy/pandas essential for vectorized financial computations.
    
    ```python
    import numpy as np; returns = np.log(prices / prices.shift(1))
    ```
12. Clean financial data: handle missing values, outliers.
    
    ```python
    data.fillna(method='ffill', inplace=True)
    ```
13. Monte Carlo for exotic option pricing.
    
    ```python
    paths = S * np.exp((r-0.5*sigma**2)*dt + sigma*np.sqrt(dt)*np.random.randn(n, steps).cumsum(1))
    payoff = np.maximum(paths[:,-1] - K, 0)
    price = np.exp(-r*T) * payoff.mean()
    ```
14. Greeks (delta, gamma, vega) via finite differences or analytic.
    
    ```python
    delta = (bs_call(S+h, K, T, r, sigma) - bs_call(S-h, K, T, r, sigma)) / (2*h)
    ```
15. Volatility smile/skew implies non-lognormal risk-neutral density.
    
    ```python
    # Breeden-Litzenberger: second derivative of call prices w.r.t. strike
    ```
16. Yield curve construction (bootstrapping bonds).
    
    ```python
    # Solve for zero rates from par bonds iteratively
    ```
17. Hazard rate models for credit default probability.
    
    ```python
    survival = np.exp(-hazard * t)
    ```
18. Mean-variance optimization (Markowitz).
    
    ```python
    from scipy.optimize import minimize
    def min_var(w): return w.T @ cov @ w
    res = minimize(min_var, w0, constraints={'type':'eq', 'fun': lambda w: w.sum()-1})
    ```
19. Black-Litterman combines views with market equilibrium.
    
    ```python
    # posterior = prior + tau * P.T @ inv(Omega) @ (views - P @ prior)
    ```
20. Value-at-Risk (VaR) via historical or parametric.
    
    ```python
    var_95 = np.percentile(port_returns, 5)
    ```
21. Backtesting quant strategies: avoid look-ahead bias.
    
    ```python
    signals = (ma_short > ma_long).astype(int).shift(1)
    strat_ret = signals * asset_ret
    ```
22. Risk parity allocates by inverse volatility.
    
    ```python
    weights = 1 / vols; weights /= weights.sum()
    ```
23. Machine learning: clustering for regime detection.
    
    ```python
    from sklearn.cluster import KMeans
    clusters = KMeans(n_clusters=3).fit(features)
    ```
24. Supervised ML for return prediction (regression/classification).
    
    ```python
    from sklearn.ensemble import RandomForestRegressor
    rf = RandomForestRegressor().fit(X_train, y_train)
    ```
25. Model validation critical: out-of-sample testing, avoid overfitting.
    
    ```python
    # Walk-forward optimization or cross-validation on time series
    ```

These capture core practical elements across foundations, options, markets, and portfolio/risk topics.

Finally, apply these **25 key takeaways** from *Python for Finance: Analyze Big Financial Data* (1st ed., Yves Hilpisch), with brief code samples based on the type of strategy to backets:

1. Python ideal for finance: prototyping to production, readable syntax.
   
   ```python
   # Simple finance example
   import numpy as np
   returns = np.random.normal(0.08, 0.15, 252)
   ```
2. Use Anaconda for scientific stack deployment.
   
   ```python
   # Typical import
   import numpy as np; import pandas as pd; import matplotlib.pyplot as plt
   ```
3. Implied volatility calculation via numerical methods.
   
   ```python
   from scipy.optimize import brentq
   def imp_vol(S, K, T, r, price): return brentq(lambda v: bs_call(S,K,T,r,v)-price, 1e-4, 5)
   ```
4. Monte Carlo simulation for option pricing.
   
   ```python
   paths = S * np.exp((r - 0.5*sigma**2)*T + sigma*np.sqrt(T)*np.random.standard_normal((steps, paths)))
   payoff = np.maximum(paths[-1] - K, 0)
   price = np.exp(-r*T) * payoff.mean()
   ```
5. Vectorization beats loops for speed.
   
   ```python
   log_returns = np.log(data / data.shift(1))  # fast
   ```
6. NumPy arrays for efficient numerical computing.
   
   ```python
   arr = np.array([1,2,3]); arr**2
   ```
7. pandas for financial time series handling.
   
   ```python
   df = pd.read_csv('data.csv', index_col=0, parse_dates=True)
   df['Return'] = df['Close'].pct_change()
   ```
8. matplotlib for 2D/3D financial visualization.
   
   ```python
   plt.plot(df['Close']); plt.title('Stock Price'); plt.show()
   ```
9. Rolling statistics for technical analysis.
   
   ```python
   df['MA50'] = df['Close'].rolling(50).mean()
   ```
10. OLS regression with statsmodels or pandas.
    
    ```python
    import statsmodels.api as sm
    model = sm.OLS(df['asset'], sm.add_constant(df['market'])).fit()
    ```
11. High-frequency data resampling.
    
    ```python
    minute_data.resample('5min').agg({'price': 'last', 'volume': 'sum'})
    ```
12. Fast I/O with PyTables for big data.
    
    ```python
    import tables as tb
    h5 = tb.open_file('data.h5', 'w')
    h5.create_array('/', 'prices', prices)
    ```
13. Performance via Cython or Numba.
    
    ```python
    from numba import jit
    @jit
    def mc_pi(n): return 4 * (np.random.rand(n)**2 + np.random.rand(n)**2 < 1).sum() / n
    ```
14. Parallel processing with multiprocessing.
    
    ```python
    from multiprocessing import Pool
    pool.map(func, args)
    ```
15. Stochastic processes: GBM simulation.
    
    ```python
    S = S0 * np.exp((r - 0.5*sigma**2)*dt + sigma*np.sqrt(dt)*np.random.randn(steps).cumsum())
    ```
16. Jump diffusion models.
    
    ```python
    jumps = np.random.poisson(lam*dt, steps) * jump_size
    ```
17. Least-squares Monte Carlo for American options.
    
    ```python
    # Regression on in-the-money paths for continuation values
    ```
18. Value-at-Risk via historical simulation.
    
    ```python
    var = np.percentile(port_returns, 5)
    ```
19. Credit valuation adjustments basics.
    
    ```python
    # Exposure simulation + default probability
    ```
20. Mean-variance portfolio optimization.
    
    ```python
    from scipy.optimize import minimize
    def port_vol(w): return np.sqrt(w.T @ cov @ w)
    cons = {'type': 'eq', 'fun': lambda w: np.sum(w)-1}
    res = minimize(port_vol, w0, constraints=cons)
    ```
21. Principal component analysis on yields.
    
    ```python
    from sklearn.decomposition import PCA
    pca = PCA(n_components=3).fit(yields)
    ```
22. Bayesian regression for parameter estimation.
    
    ```python
    # Use pymc3 or similar for priors/updates
    ```
23. Excel integration via xlwings or openpyxl.
    
    ```python
    import xlwings as xw
    wb = xw.Book('data.xlsx')
    ```
24. Object-oriented framework for derivatives valuation.
    
    ```python
    class Valuation:
        def generate_payoff(self): pass
        def present_value(self): pass
    ```
25. End-to-end Monte Carlo derivatives analytics case study.
    
    ```python
    # Combine simulation, valuation, risk metrics in classes
    ```

These capture core practical elements from the book’s focus on data handling, analytics, simulation, and performance in finance (1st edition emphasis).
