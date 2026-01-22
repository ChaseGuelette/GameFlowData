try:
    import scipy.stats  # noqa: F401

    print("scipy available")
except ImportError:
    print("scipy NOT available")

try:
    import sklearn.linear_model  # noqa: F401

    print("sklearn available")
except ImportError:
    print("sklearn NOT available")
