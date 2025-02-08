import requests
import csv
import pandas as pd

# ✅ Open-Meteo API (No API Key Required)
URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"


### **🔹 Part 1: Fetch Weather Data (Extract)**
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)

    if response.status_code == 200:
        data = response.json()
        if "hourly" in data and "temperature_2m" in data["hourly"]:
            return data["hourly"]
        else:
            print("⚠️ Warning: 'hourly' data missing in API response.")
            return None
    else:
        print(f"❌ Error: Unable to fetch data. Status code: {response.status_code}")
        return None


### **🔹 Part 2: Save Data to CSV (Load)**
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    if not data:
        print("⚠️ No data available to save.")
        return

    with open(filename, "w", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # ✅ Write header row
        writer.writerow(["Time", "Temperature", "Humidity", "Wind Speed"])

        # ✅ Write data rows
        for i in range(len(data["time"])):
            writer.writerow([
                data["time"][i],
                data["temperature_2m"][i],
                data["relative_humidity_2m"][i],
                data["wind_speed_10m"][i]
            ])

    print(f"✅ Weather data saved to {filename}")


### **🔹 Part 3: Clean Data (Transform)**
def clean_data(input_file, output_file):
    """Cleans weather data based on rules:
        - Temperature: 0 to 60°C
        - Humidity: 0% to 80%
        - Wind Speed: 3 to 150 km/h
    """
    df = pd.read_csv(input_file)

    # ✅ Remove invalid values
    df = df[(df["Temperature"] >= 0) & (df["Temperature"] <= 60)]
    df = df[(df["Humidity"] >= 0) & (df["Humidity"] <= 80)]
    df = df[(df["Wind Speed"] >= 3) & (df["Wind Speed"] <= 150)]

    # ✅ Fill missing values, but ignore 'Time' column
    numeric_columns = ["Temperature", "Humidity", "Wind Speed"]
    df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].mean())

    # ✅ Save cleaned data
    df.to_csv(output_file, index=False)
    print(f"✅ Cleaned data saved to {output_file}")



### **🔹 Part 4: Summarize Data (Aggregation)**
def summarize_data(filename):
    """Summarizes weather data including averages and extremes."""
    df = pd.read_csv(filename)

    if df.empty:
        print("⚠️ No data available to summarize.")
        return

    # ✅ Compute statistics
    summary = {
        "Total Records": len(df),
        "Avg Temperature": df["Temperature"].mean(),
        "Max Temperature": df["Temperature"].max(),
        "Min Temperature": df["Temperature"].min(),
        "Avg Humidity": df["Humidity"].mean(),
        "Avg Wind Speed": df["Wind Speed"].mean(),
    }

    # ✅ Print summary
    print("\n📊 Weather Data Summary 📊")
    for key, value in summary.items():
        print(f"{key}: {value:.2f}")


### **🔹 Run the Data Pipeline**
if __name__ == "__main__":
    weather_data = fetch_weather_data()

    if weather_data:
        save_to_csv(weather_data, "weather_data.csv")
        clean_data("weather_data.csv", "cleaned_data.csv")
        summarize_data("cleaned_data.csv")
