"""兼容性 shim：CorrCapPortfolio 已迁移至 portfolio.sizing.corr_cap.CorrCapSizer。"""

from portfolio.sizing.corr_cap import CorrCapSizer as CorrCapPortfolio

__all__ = ["CorrCapPortfolio"]
