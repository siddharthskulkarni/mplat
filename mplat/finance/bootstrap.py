import pandas as pd
from scipy.optimize import newton

def bootstrap_spot_rates(df):
    """
    Bootstrap semi-annual spot rates (S_n) from par rates (C_n).

    Parameters
    ----------
    df : pd.DataFrame
        Columns: 'Date', then maturities in days (float)
        Values: par rates in decimals (e.g., 0.0429 for 4.29%)

    Returns
    -------
    pd.DataFrame
        Same shape as df, with par rates replaced by bootstrapped spot rates S_n.
    """
    spot_df = df.copy()
    
    for idx, row in df.iterrows():
        S = {}  # dictionary for spot rates: key = t_i, value = S_i
        
        # consider maturities >= 1 year (365 days) for semi-annual bootstrapping
        maturities = sorted([c for c in df.columns if c != 'Date' and c >= 365])
        
        for n in maturities:
            C_n = row[n]          # par/coupon rate
            t_n = n / 365         # time to maturity in years
            
            # discounted previous coupons
            sum_discounted_coupons = sum(
                C_n / (1 + S_i)**t_i
                for t_i, S_i in S.items()
            )
            
            # semi-annual bootstrapping
            S_n = ((1 + C_n/2) / (1 - sum_discounted_coupons))**(1/(2*t_n)) - 1
            
            S[t_n] = S_n
            spot_df.at[idx, n] = S_n
            
    return spot_df


def compute_ytm_from_spot(df):
    """
    Compute Yield to Maturity (Y_n) from bootstrapped spot rates (semi-annual compounding).

    Parameters
    ----------
    df : pd.DataFrame
        Columns: 'Date', then maturities in days (float)
        Values: spot rates (decimal)

    Returns
    -------
    pd.DataFrame
        Same shape as df, with yields to maturity (annual, semi-annual compounding)
    """
    ytm_df = df.copy()
    
    for idx, row in df.iterrows():
        maturities = sorted([c for c in df.columns if c != 'Date'])
        
        for n in maturities:
            # Collect spot rates up to current maturity
            S = {t_i/365: row[m] for m, t_i in zip(maturities, maturities) if m <= n}
            
            t_n = n / 365
            C_n = row[n]  # coupon for this maturity
            
            # Define function for root-finding (price = 1)
            def f(Y):
                return sum(
                    C_n/2 / (1 + Y/2)**(2*t_i) for t_i in S
                ) + (1 + C_n/2) / (1 + Y/2)**(2*t_n) - 1
            
            # Solve numerically for YTM
            Y_n = newton(f, x0=0.03)  # initial guess 3%
            ytm_df.at[idx, n] = Y_n
            
    return ytm_df