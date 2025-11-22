import pandas as pd



def wind_categorization(df):
    """
    Use bining to create wind categories
    Bins are defined with their corresponding labels

    """
    bins = [ 0, 1.5, 3.3, 5.4, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6, float("inf")] #upper limit infinity
    labels = ["Calm", "Light Air", "Light Breeze", "Gentle Breeze",
        "Moderate Breeze", "Fresh Breeze", "Strong Breeze",
        "Near Gale", "Gale", "Strong Gale", "Storm", "Violent Storm"
    ]

    df["wind_strength"] = pd.cut( #assign each row to the corresponding interval - apply matching label
        df["Wind Speed (km/h)"],
        bins=bins,
        labels = labels,
        right = True #right limit included
    )

    return df

if __name__ == "__main__":
    print("Wind strength categorization ok")