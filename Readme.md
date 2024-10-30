# Hurricane Agriculture Data

*** NOTICE ***
The project is incomplete. The project will be completed by December when October and November 2024 data is analyzed and reviewed.
***

## Description
This project focuses on analyzing the stock market response in the agriculture sector during hurricane seasons, particularly when Category 3, 4, and 5 hurricanes made landfall in southeastern U.S. states (Florida, Georgia, Alabama, Mississippi, Louisiana, South Carolina, North Carolina). By examining historical stock data from 2005 to 2024 and correlating it with major hurricane landfalls, the analysis will highlight how these extreme weather events impacted agricultural companies. 

The project aims to:
- Identify short-term stock trends during the hurricane season (June-November).
- Compare the market responses across different years, with a particular emphasis on the 2024 hurricane season.
- Provide insights into whether the severity and frequency of these hurricanes have caused noticeable shifts in the agriculture sector's stock performance over time.

## Installation Instructions
This project is implemented in Python. The following packages are required:
- `boto3`
- `requests`
- `csv`
- `os`
- `time`
- `logging`
- `botocore.exceptions`
- `base64`
- `pandas`
- `matplotlib`

You can install the required packages using pip:
```bash
pip install boto3 requests pandas matplotlib
```

## Usage

To run this project, you will need an AWS Account to access AWS services. The services used include:

    S3
    KMS
    Athena
    CloudWatch
    Tableau Public

## Data Sources

This project leverages multiple data sources for analysis:

    Alpha Vantage API: Used to retrieve stock market data for the top 50 agriculture-related stocks.
    List of Costliest Atlantic Hurricanes: Historical data on the most financially damaging hurricanes.
    Additional Resources on Major Hurricanes:
        Time: Strongest Hurricanes in U.S. History
        CBS News: Most Destructive Hurricanes in U.S. History
        Reuters: Most Devastating Hurricanes in U.S. History
    The Most Lucrative Food Crop in Each State: Vivid Maps
    Continental United States Hurricane Impacts/Landfalls 1851-2023: NOAA
    World Bank Group Commodity Markets: World Bank

## Analysis Methodology

Data for this project was primarily collected using the Alpha Vantage API and World Bank Pink Data. The analysis focuses on various agriculture-related companies categorized into:

    Grains & Commodities
        ADM (Archer-Daniels-Midland Company)
        BG (Bunge Limited)
        ANDE (The Andersons, Inc.)
        MOS (Mosaic Company)
        CF (CF Industries Holdings, Inc.)
        ALCO (Alico, Inc.)
        FLO (Flowers Foods, Inc.)

    Poultry & Meats
        TSN (Tyson Foods, Inc.)
        BRFS (BRF S.A.)
        CALM (Cal-Maine Foods, Inc.)
        HRL (Hormel Foods Corporation)
        PPC (Pilgrim's Pride Corporation)
        SEB (Seaboard Corporation)

    Packaged & Processed Foods
        CAG (Conagra Brands, Inc.)
        HAIN (Hain Celestial Group, Inc.)
        MKC (McCormick & Company, Inc.)
        MDLZ (Mondelez International, Inc.)
        UL (Unilever PLC)
        WW (WW International, Inc.)
        BGS (B&G Foods, Inc.)
        THS (TreeHouse Foods, Inc.)

    Produce & Natural Foods
        CVGW (Calavo Growers, Inc.)
        FDP (Fresh Del Monte Produce Inc.)
        UNFI (United Natural Foods, Inc.)
        JVA (Coffee Holding Co., Inc.)

    Agricultural Equipment & Irrigation
        DE (Deere & Company)
        AGCO (AGCO Corporation)
        LNN (Lindsay Corporation)

    Ingredients & Specialty Products
        DAR (Darling Ingredients Inc.)
        INGR (Ingredion Incorporated)

    Lawn & Garden
        SMG (Scotts Miracle-Gro Company)
        STKL (SunOpta Inc.)

    Retail & Supply Chain
        TSCO (Tractor Supply Company)
        SYY (Sysco Corporation)

    Miscellaneous
        LANC (Lancaster Colony Corporation)
        JJSF (J&J Snack Foods Corp.)
        SENEA (Seneca Foods Corporation)

The commodities analyzed include:

    Cotton
    Soybeans
    Maize (Corn)
    Rice
    Orange
    Beef, Chicken, and Shrimp
    Sugar

## Results and Visualizations

The analysis results are visualized using Tableau Public. You can explore the story:
[View the Tableau Story](https://public.tableau.com/app/profile/mathewos.yohannes/viz/hurricaneagriculturedata3/Story1)
