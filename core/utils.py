"""Contains all helper functions"""


def api_to_conversion_factor(api):
    """Convert from API to Conversion Factor"""
    cf = 6.28981 / (141.5 / (api + 131.5))
    return cf
