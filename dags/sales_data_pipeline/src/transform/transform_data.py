import pandas as pd

def clean_data(df):

    df.dropna(subset=['Year'], inplace=True)

    # I MODIFY THE YEAR COLUMN SINCE FLOAT IS NOT NECESSARY
    df['Year'] = df['Year'].astype(int)

    df['Total_Sales'] = df['NA_Sales'] + df['EU_Sales'] + df['JP_Sales'] + df['Other_Sales']

    return df
 

def aggregate_data(df):

    # GROUP BY PLATFORM AND YEAR TO ANALYZE SALES
    sales_by_platform_year = df.groupby(["Platform","Year"])["Total_Sales"].sum().reset_index().sort_values(by="Year", ascending=True)

    return sales_by_platform_year