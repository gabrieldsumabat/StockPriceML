# StockPriceML

Exploration of Machine Learning Models in Predicting the Closing Price using Technical Indicators.

#Set Up

1. Go to Alpha Vantage to generate an API Key : https://www.alphavantage.co/support/#api-key
2. Configure Environment Variable (AV_API_KEY) for Alpha Vantage API Key
3. Run Maven Install to download all dependencies.

#Generated Directories

This project will generate a ./csv/ directory to store the fetched data.
There is an API call limit thus a specified delay between batched API is implemented.