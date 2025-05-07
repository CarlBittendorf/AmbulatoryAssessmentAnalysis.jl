
# 1. Exports
# 2. Generic Definitions
# 3. Concrete Implementations
# 4. Helper Functions
# 5. High-level Functions

####################################################################################################
# EXPORTS
####################################################################################################

export OpenMeteo, OpenMeteoHistoricalWeather, OpenMeteoWeatherForecast, OpenMeteoAirQuality
export url

####################################################################################################
# GENERIC DEFINITIONS
####################################################################################################

abstract type OpenMeteo end

"""
    url(::Type{<:OpenMeteo})

Return the url for the API endpoint for the given type of Open-meteo data.
"""
function url end

####################################################################################################
# CONCRETE IMPLEMENTATIONS
####################################################################################################

# https://open-meteo.com/en/docs/historical-weather-api
struct OpenMeteoHistoricalWeather <: OpenMeteo end
# https://open-meteo.com/en/docs
struct OpenMeteoWeatherForecast <: OpenMeteo end
# https://open-meteo.com/en/docs/air-quality-api
struct OpenMeteoAirQuality <: OpenMeteo end

url(::Type{OpenMeteoHistoricalWeather}) = "https://archive-api.open-meteo.com/v1/archive"
url(::Type{OpenMeteoWeatherForecast}) = "https://api.open-meteo.com/v1/forecast"
url(::Type{OpenMeteoAirQuality}) = "https://air-quality-api.open-meteo.com/v1/air-quality"

####################################################################################################
# HELPER FUNCTIONS
####################################################################################################

_openmeteo_preprocess(x) = x isa Vector ? join(string.(x), ",") : string(x)

function _openmeteo_download(url; timeout::Int = 180, sleeptime = 10, args...)
    query = [string(first(x)) => _openmeteo_preprocess(last(x)) for x in args]

    start = now()

    while now() - start < Millisecond(timeout * 1000)
        response = HTTP.get(url; query, status_exception = false)

        if response.status == 200
            return @chain response.body begin
                String
                JSON.parse
            end
        else
            sleep(sleeptime)
        end
    end

    throw(HTTP.Exceptions.TimeoutError(timeout))
end

function _openmeteo_cluster(x; maxgap = 3)
    indices = sort(eachindex(x); by = i -> x[i])
    labels = repeat([uuid4()], length(x))

    uuid = first(labels)

    for (i, date) in enumerate(x[indices])
        i == 1 && continue

        if date - x[indices][i - 1] > Day(maxgap)
            uuid = uuid4()
        end

        labels[indices][i] = uuid
    end

    return labels
end

function _openmeteo_chunk(x; maxlength = 1000)
    labels = zeros(Int, length(x))

    index = labels[1] = 1

    for (i, l) in enumerate(x)
        labels[i] != 0 && continue

        if sum(x[labels .== index]) + l > maxlength || count(labels .== index) >= 50
            index += 1
        end

        labels[i] = index
    end

    return labels
end

####################################################################################################
# HIGH-LEVEL FUNCTIONS
####################################################################################################

"""
    download(T, latitude, longitude; args...) where {T <: OpenMeteo}
    download(T, latitude, longitude, start_date, end_date; args...) where {T <: OpenMeteo}

Download Open-meteo data of type `T`.

The keywords `daily`, `hourly` and `current` are available to select the desired variables.
For information on available variables, see the Open-meteo documentation:
* https://open-meteo.com/en/docs/historical-weather-api
* https://open-meteo.com/en/docs
* https://open-meteo.com/en/docs/air-quality-api

# Examples

```julia
julia> download(OpenMeteoWeatherForecast, 49.0069, 8.4037; hourly = ["temperature_2m", "rain"])

julia> download(OpenMeteoHistoricalWeather, 49.0069, 8.4037, "2020-01-01", "2025-01-01"; daily = "weather_code")

julia> download(OpenMeteoAirQuality, 49.0069, 8.4037; current = "pm10")
```
"""
function Base.download(::Type{T}, latitude, longitude;
        timezone = "auto", args...) where {T <: OpenMeteo}
    _openmeteo_download(url(T); latitude, longitude, timezone, args...)
end

function Base.download(::Type{T}, latitude, longitude, start_date, end_date;
        timezone = "auto", args...) where {T <: OpenMeteo}
    _openmeteo_download(
        url(T); latitude, longitude, start_date, end_date, timezone, args...)
end

"""
    download(T, df; hourly, daily, args...) where {T <: OpenMeteo} -> DataFrame

Download Open-meteo data of type `T` for each row in `df`.

The `timecol`, `latitudecol` and `longitudecol` columns contain the timestamps, latitudes
and longitudes, respectively.

Tip: The download time can often be shortened by rounding the GPS coordinates to one or two decimal places.
"""
function Base.download(
        ::Type{T}, df::DataFrame; hourly = [], daily = [], timecol = :DateTime,
        latitudecol = :Latitude, longitudecol = :Longitude, args...) where {T <: OpenMeteo}
    # unique locations in the dataframe
    df_locations = @chain df begin
        groupby([latitudecol, longitudecol])
        combine(first)
    end

    latitudes, longitudes = df_locations.Latitude, df_locations.Longitude
    today = Date(now())

    # determine coordinates of the corresponding grid cell for each location
    N = length(latitudes)
    grid_latitudes, grid_longitudes = zeros(N), zeros(N)

    @showprogress "Determining Open-meteo grid cells..." for indices in chunk(N, 250)
        result = Base.download(T, latitudes[indices], longitudes[indices], today, today)

        grid_latitudes[indices] = getindex.(result, "latitude")
        grid_longitudes[indices] = getindex.(result, "longitude")
    end

    df_grid = DataFrame(
        latitudecol => latitudes,
        longitudecol => longitudes,
        :GridLatitude => grid_latitudes,
        :GridLongitude => grid_longitudes
    )

    df_chunks = @chain df begin
        leftjoin(df_grid; on = [latitudecol, longitudecol])
        transform(timecol => ByRow(Date) => :Date)

        groupby([:GridLatitude, :GridLongitude])
        transform(:Date => _openmeteo_cluster => :Cluster)

        groupby(:Cluster)
        combine(
            [:GridLatitude, :GridLongitude] .=> first,
            :Date => minimum => :StartDate,
            :Date => maximum => :EndDate;
            renamecols = false
        )
        transform([:StartDate, :EndDate] => ByRow((s, e) -> Dates.value(e - s) + 1) => :Length)
        transform(:Length => _openmeteo_chunk => :Chunk)
    end

    dicts = repeat([[Dict{String, Any}()]], length(unique(df_chunks.Chunk)))

    @showprogress "Downloading Open-meteo data..." for i in eachindex(dicts)
        df_chunk = subset(df_chunks, :Chunk => ByRow(isequal(i)))
        _, latitude, longitude, start_date, end_date = eachcol(df_chunk)

        result = Base.download(T, latitude, longitude, start_date, end_date;
            hourly, daily, args...)

        dicts[i] = result
    end

    grid_latitudes = vcat((subset(df_chunks, :Chunk => ByRow(isequal(i))).GridLatitude for i in eachindex(dicts))...)
    grid_longitudes = vcat((subset(df_chunks, :Chunk => ByRow(isequal(i))).GridLongitude for i in eachindex(dicts))...)

    if !isempty(daily)
        df_daily = @chain dicts begin
            vcat(_...)
            map(
                i -> DataFrame(
                    _[i]["daily"]...,
                    "GridLatitude" => grid_latitudes[i],
                    "GridLongitude" => grid_longitudes[i]
                ),
                eachindex(_)
            )
            vcat(_...)
            transform(:time => ByRow(Date) => :Date)
            select(Not(:time))
        end
    else
        df_daily = DataFrame([name => [] for name in [:Date, :GridLatitude, :GridLongitude]])
    end

    if !isempty(hourly)
        df_hourly = @chain dicts begin
            vcat(_...)
            map(
                i -> DataFrame(
                    _[i]["hourly"]...,
                    "GridLatitude" => grid_latitudes[i],
                    "GridLongitude" => grid_longitudes[i]
                ),
                eachindex(_)
            )
            vcat(_...)
            transform(:time => ByRow(DateTime) => :DateTimeHourly)
            select(Not(:time))
        end
    else
        df_hourly = DataFrame([name => []
                               for name in [:DateTimeHourly, :GridLatitude, :GridLongitude]])
    end

    @chain df begin
        leftjoin(df_grid; on = [latitudecol, longitudecol])

        transform(
            timecol => ByRow(Date) => :Date,
            timecol => ByRow(x -> round(DateTime(x), Hour)) => :DateTimeHourly
        )
        leftjoin(df_daily; on = [:Date, :GridLatitude, :GridLongitude])
        leftjoin(df_hourly; on = [:DateTimeHourly, :GridLatitude, :GridLongitude])
    end
end

function Base.download(::Type{T}, datetime::AbstractVector{Union{Date, DateTime}},
        latitude, longitude; hourly = [], daily = [], args...) where {T <: OpenMeteo}
    Base.download(
        T, DataFrame(:DateTime => datetime, :Latitude => latitude, :Longitude => longitude);
        hourly, daily, args...
    )
end