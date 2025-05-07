
@testset "Open-meteo Download" begin
    Random.seed!(123)

    today = Date(now())

    df = DataFrame(
        :DateTime => rand(range(today - Day(100), today - Day(50)), 500),
        :Latitude => rand(47:0.1:50, 500),
        :Longitude => rand(7:0.1:10, 500)
    )

    historical_weather_daily = [
        "weather_code", "temperature_2m_mean", "temperature_2m_max",
        "temperature_2m_min", "apparent_temperature_mean",
        "apparent_temperature_max", "apparent_temperature_min", "precipitation_sum",
        "precipitation_hours", "sunrise", "sunset", "sunshine_duration",
        "daylight_duration", "wind_speed_10m_max", "wind_gusts_10m_max",
        "wind_direction_10m_dominant", "shortwave_radiation_sum"]
    air_quality_hourly = [
        "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide",
        "ozone", "carbon_dioxide", "ammonia", "aerosol_optical_depth", "methane", "dust"]

    @test download(OpenMeteoHistoricalWeather, df; daily = historical_weather_daily) isa
          DataFrame
    @test download(OpenMeteoAirQuality, df; hourly = air_quality_hourly) isa
          DataFrame
end