import pandas as pd
import datetime

apy = pd.read_csv("apy.csv")

print apy.columns.values

weather_2012 = pd.read_csv("weather data/Punjab/weather_data_punjab_2012.csv")
weather_2013 = pd.read_csv("weather data/Punjab/weather_data_punjab_2013.csv")

weather_yield_2013 = weather_2012.append(weather_2013)

weather_yield_2013 = pd.merge(weather_yield_2013,apy,on="District_Name")

weather_yield_2013 = weather_yield_2013[(weather_yield_2013["Crop_Year"] == 2013) & (weather_yield_2013["Crop"] == "Wheat")]

weather_yield_2013 = weather_yield_2013[(weather_yield_2013["Date"] >= '2012-10-22') & (weather_yield_2013["Date"] <= '2013-04-22')]

# print weather_yield_2013.groupby(["District_Name"])["Date"].count()

weather_yield_2013["Date"] = pd.to_datetime(weather_yield_2013["Date"])
weather_yield_2013["daysInSeason"] = weather_yield_2013["Date"] - datetime.date(2012, 10, 22)
weather_yield_2013["daysInSeason"] = weather_yield_2013["daysInSeason"].apply(lambda x: x.days)

# d1 = datetime.date(2012, 10, 22)  # start date
# d2 = datetime.date(2013, 04, 22)  # end date

# delta = d2 - d1

# for i in range(delta.days + 1):
# 	d = d1 + datetime.timedelta(i)
# 	print d.strftime("%d-%m-%Y")
# 	weather_yield_2013[weather_yield_2013["Date"] == str(d.strftime("%d-%m-%Y"))]["daysInSeason"] = i+1

print weather_yield_2013
# weather_yield_2013.to_csv("winter_wheat_2013.csv",index=False)
