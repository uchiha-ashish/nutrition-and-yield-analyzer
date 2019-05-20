import requests
import time, sys
import datetime
import pandas as pd

def get_value(jsonObj,query):
	if query in jsonObj:
		return jsonObj[query]
	else:
		return None

d1 = datetime.date(2012, 6, 2)  # start date
d2 = datetime.date(2012, 6, 12)  # end date

df = pd.read_csv("apy.csv")
d = {'State_name': df["State_Name"], 'District_Name' : df["District_Name"], 'Crop_Year' : df["Crop_Year"]}
df1 = pd.DataFrame(d)
df1 = df1.drop_duplicates()
req_states = ['Punjab']
df1 = df1[df1.State_name.isin(req_states)]

df2 = pd.read_csv("district_geo_cordinates.csv")
x = df1["District_Name"].unique().tolist()
df2["District_Name"] = df2["District_Name"].str.strip()
df2["Geocode"] = df2["Geocode"].str.strip()
df2 = df2[df2["District_Name"].isin(x)]

locations = df2["Geocode"].tolist() 

# print locations
# sys.exit(0)

delta = d2 - d1

df = pd.DataFrame(columns = ['Latitude', 'Longitude', 'Date', 'apparentTemperatureMax', 
	'apparentTemperatureMin', 'cloudCover', 'dewPoint', 'humidity', 'precipIntensity', 
	'precipIntensityMax', 'precipProbability', 'precipAccumulation','precipType', 
	'pressure', 'temperatureMax', 'temperatureMin', 'visibility', 'windBearing', 'windSpeed']) 

dev_keys_data = pd.read_csv("Darksky Developer key - Sheet.csv")

count = 0
dev_key_number = 0
key_use_count = dev_keys_data.loc[dev_key_number]["Usage count"]
for i in range(delta.days + 1):
	for l in locations:
	    d = d1 + datetime.timedelta(i)
	    unixtime = int(time.mktime(d.timetuple()))
	    while key_use_count >= 999:
	    	dev_key_number += 1
	    	key_use_count = dev_keys_data.loc[dev_key_number]["Usage count"]
	    dev_key = dev_keys_data.loc[dev_key_number]["developer key url"]
	    req = dev_key+l+","+str(unixtime)+"?exclude=currently,minutely,hourly"
	 
	    print d, unixtime, dev_key_number, key_use_count, l
	    try:
	    	response = requests.get(req)
	    	key_use_count += 1
	    	dev_keys_data.set_value(dev_key_number,"Usage count",key_use_count)
	    	dev_keys_data.to_csv("Darksky Developer key - Sheet.csv", index=False)

	    	if response.status_code == 200:
	    		data = response.json()
		    	if "daily" in data and "data" in data["daily"] and len(data["daily"]["data"]) > 0:
		    		weather_data = data["daily"]["data"][0]
		    		df.loc[count] = [data["latitude"], 
			    	data["longitude"], 
			    	d, 
			    	get_value(weather_data,"apparentTemperatureHigh"), 
			    	get_value(weather_data,"apparentTemperatureLow"),
			    	get_value(weather_data,"cloudCover"),
			    	get_value(weather_data,"dewPoint"),
			    	get_value(weather_data,"humidity"),
			    	get_value(weather_data,"precipIntensity"),
			    	get_value(weather_data,"precipIntensityMax"),
			    	get_value(weather_data,"precipProbability"),
			    	get_value(weather_data,"precipAccumulation"),
			    	get_value(weather_data,"precipType"),
			    	get_value(weather_data,"pressure"),
			    	get_value(weather_data,"temperatureHigh"),
			    	get_value(weather_data,"temperatureLow"),
			    	get_value(weather_data,"visibility"),
			    	get_value(weather_data,"windBearing"),
			    	get_value(weather_data,"windSpeed")]
		    	else:
		    		print "Daily data not available for this date and location\n"
		    	count += 1
	    	else:
		    	print response.status_code
		    	print response
	    except Exception as e:
	    	print e
	    	

	df.to_csv('weather_data_punjab_2012_3.csv')
