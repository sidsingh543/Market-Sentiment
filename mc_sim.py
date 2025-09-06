import numpy as np

def gbm_mc(
    s0: float,
    mu_annual: float,
    sigma_annual: float,
    days: int,
    paths: int,
    seed: int | None = None,
):
    if seed is not None:
        np.random.seed(seed)

    dt = 1.0/252.0
    mu = mu_annual
    sigma = sigma_annual

    z = np.random.normal(size=(days, paths))
    increments = (mu - 0.5*sigma**2)*dt + sigma*np.sqrt(dt)*z
    log_paths = np.cumsum(increments, axis=0)
    log_paths = np.vstack([np.zeros((1, paths)), log_paths])
    s_paths = s0 * np.exp(log_paths)
    return s_paths
