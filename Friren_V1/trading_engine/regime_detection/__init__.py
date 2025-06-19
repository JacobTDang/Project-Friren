try:
    from .regime_market import EnhancedRegimeDetector
except ImportError as e:
    print(f"Warning: Could not import EnhancedRegimeDetector: {e}")
    EnhancedRegimeDetector = None

try:
    from .entropy_regime_detector import EntropyRegimeDetector
except ImportError as e:
    print(f"Warning: Could not import EntropyRegimeDetector: {e}")
    EntropyRegimeDetector = None

# Build __all__ dynamically
__all__ = []
if EnhancedRegimeDetector is not None:
    __all__.append('EnhancedRegimeDetector')
if EntropyRegimeDetector is not None:
    __all__.append('EntropyRegimeDetector')
