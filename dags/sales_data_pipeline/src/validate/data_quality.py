import pandas as pd

def validate_clean_data(dataframe):

    if dataframe["Year"].isnull().sum():
        raise ValueError("Year contains null values")
    
    if not pd.api.types.is_integer_dtype(dataframe["Year"]):
        raise TypeError("Year is not an integer type")
    
    negatives = (dataframe["Total_Sales"] < 0).sum()

    if negatives > 0:
        raise ValueError("Total_Sales contains negative values")
    


def validate_aggregated_data(dataframe):

    if dataframe.duplicated(subset=["Platform","Year"]).sum() > 0:
        raise ValueError("Duplicate Platform-Year combinations found")
    
    negatives = (dataframe["Total_Sales"] < 0).sum()
    if negatives > 0:
        raise ValueError("Total_Sales contains negative values")
