
# 1. Exports
# 2. Generic Definitions
# 3. Concrete Implementations
# 4. Helper Functions
# 5. High-level Functions

# Further information: https://manual.m-path.io/knowledge-base/mobile-sensing-full/

####################################################################################################
# EXPORTS
####################################################################################################

export MPathMobileSensing, MPathAccelerationFeatures, MPathAmbientLight, MPathAppUsage,
       MPathBattery, MPathConnectivity, MPathDeviceInformation, MPathDisplay, MPathLocation,
       MPathMemory, MPathPhysicalActivity, MPathSteps, MPathTimeZone, MPathWeather,
       MPathWiFi
export typename, variablenames, process

####################################################################################################
# GENERIC DEFINITIONS
####################################################################################################

abstract type MPathMobileSensing end

"""
    typename(::Type{<:MPathMobileSensing}) -> String

Return the name of the Copenhagen Research Platform (CARP) identifier that is associated
with this type of mobile sensing data.

# Examples

```jldoctest
julia> typename(MPathLocation)
"dk.cachet.carp.location"

julia> typename(MPathSteps)
"dk.cachet.carp.stepcount"
```
"""
function typename end

"""
    variablenames(::Type{<:MPathMobileSensing}) -> Vector{String}

Return the names of the variables belonging to this type of mobile sensing data.

# Examples

```jldoctest
julia> variablenames(MPathLocation)
12-element Vector{String}:
 "MPathConnectionID"
 "DateTime"
 "Latitude"
 "Longitude"
 ⋮
 "SpeedAccuracy"
 "Heading"
 "LocationDateTime"
 "IsMock"

julia> variablenames(MPathDisplay)
3-element Vector{String}:
 "MPathConnectionID"
 "DateTime"
 "DisplayOn"
```
"""
function variablenames end

"""
    process(df, ::Type{<:MPathMobileSensing}) -> DataFrame

Process the given `DataFrame`, i.e. rename columns.
"""
function process end

process(df, ::Type{<:MPathMobileSensing}) = df

"""
    load(path, ::Type{<:MPathMobileSensing}) -> DataFrame

Read and process the given mobile sensing file and return a `DataFrame`.
"""
function load end

function load(path, ::Type{T}) where {T <: MPathMobileSensing}
    _mpath_load(path, _mpath_connection_id(path), T)
end

load(T::Type{<:MPathMobileSensing}) = Base.Fix2(load, T)

####################################################################################################
# CONCRETE IMPLEMENTATIONS
####################################################################################################

struct MPathAccelerationFeatures <: MPathMobileSensing end
struct MPathAmbientLight <: MPathMobileSensing end
struct MPathAppUsage <: MPathMobileSensing end
struct MPathBattery <: MPathMobileSensing end
struct MPathConnectivity <: MPathMobileSensing end
struct MPathDeviceInformation <: MPathMobileSensing end
struct MPathDisplay <: MPathMobileSensing end
struct MPathLocation <: MPathMobileSensing end
struct MPathMemory <: MPathMobileSensing end
struct MPathPhysicalActivity <: MPathMobileSensing end
struct MPathSteps <: MPathMobileSensing end
struct MPathTimeZone <: MPathMobileSensing end
struct MPathWeather <: MPathMobileSensing end
struct MPathWiFi <: MPathMobileSensing end

typename(::Type{MPathAccelerationFeatures}) = "dk.cachet.carp.accelerationfeatures"
typename(::Type{MPathAmbientLight}) = "dk.cachet.carp.ambientlight"
typename(::Type{MPathAppUsage}) = "dk.cachet.carp.appusage"
typename(::Type{MPathBattery}) = "dk.cachet.carp.batterystate"
typename(::Type{MPathConnectivity}) = "dk.cachet.carp.connectivity"
typename(::Type{MPathDeviceInformation}) = "dk.cachet.carp.deviceinformation"
typename(::Type{MPathDisplay}) = "dk.cachet.carp.screenevent"
typename(::Type{MPathLocation}) = "dk.cachet.carp.location"
typename(::Type{MPathMemory}) = "dk.cachet.carp.freememory"
typename(::Type{MPathPhysicalActivity}) = "dk.cachet.carp.activity"
typename(::Type{MPathSteps}) = "dk.cachet.carp.stepcount"
typename(::Type{MPathTimeZone}) = "dk.cachet.carp.timezone"
typename(::Type{MPathWeather}) = "dk.cachet.carp.weather"
typename(::Type{MPathWiFi}) = "dk.cachet.carp.wifi"

# define methods via metaprogramming to avoid repetitive code
for (type, names) in [
    MPathAccelerationFeatures => [
        "NumberOfSamples", "MeanX", "MeanY", "MeanZ", "StdX", "StdY",
        "StdZ", "AverageAbsoluteDifferenceX", "AverageAbsoluteDifferenceY",
        "AverageAbsoluteDifferenceZ", "MinX", "MinY", "MinZ", "MaxX", "MaxY", "MaxZ",
        "MaxMinDifferenceX", "MaxMinDifferenceY", "MaxMinDifferenceZ", "MedianX",
        "MedianY", "MedianZ", "MadX", "MadY", "MadZ", "InterQuartileRangeX",
        "InterQuartileRangeY", "InterQuartileRangeZ", "CountNegativeX",
        "CountNegativeY", "CountNegativeZ", "CountPositiveX", "CountPositiveY",
        "CountPositiveZ", "CountAboveMeanX", "CountAboveMeanY", "CountAboveMeanZ",
        "EnergyX", "EnergyY", "EnergyZ", "MeanAcceleration", "SignalMagnitudeArea"],
    MPathAmbientLight => ["MeanLux", "StdLux", "MinLux", "MaxLux"],
    MPathAppUsage => [
        "AppUsageStart", "AppUsageEnd", "AppName", "AppShortName",
        "AppUsageDuration", "AppStart", "AppEnd", "AppLastForeground"],
    MPathBattery => ["BatteryLevel", "BatteryCharging"],
    MPathConnectivity => ["ConnectivityStatus"],
    MPathDeviceInformation => [
        "Platform", "DeviceID", "Hardware", "DeviceName",
        "DeviceManufacturer", "DeviceModel", "OperatingSystem"],
    MPathDisplay => ["DisplayOn"],
    MPathLocation => [
        "Latitude", "Longitude", "Altitude", "LocationAccuracy", "VerticalAccuracy",
        "Speed", "SpeedAccuracy", "Heading", "LocationDateTime", "IsMock"],
    MPathPhysicalActivity => ["PhysicalActivityType", "PhysicalActivityConfidence"],
    MPathMemory => ["FreePhysicalMemory", "FreeVirtualMemory"],
    MPathSteps => ["CumulativeSteps"],
    MPathTimeZone => ["TimeZone"],
    MPathWeather => [
        "Country", "AreaName", "WeatherMain", "WeatherDescription",
        "WeatherDateTime", "Sunrise", "Sunset", "Latitude",
        "Longitude", "Pressure", "WindSpeed", "WindDegree", "Humidity",
        "Cloudiness", "Temperature", "MinTemperature", "MaxTemperature"],
    MPathWiFi => ["WiFiIPAddress"]
]
    # MPathConnectionID, DateTime are always the first two columns
    pushfirst!(names, "MPathConnectionID", "DateTime")

    @eval variablenames(::Type{$type}) = $names
end

function process(df, ::Type{MPathAccelerationFeatures})
    safe_rename(
        df,
        "count" => "NumberOfSamples",
        "xMean" => "MeanX",
        "yMean" => "MeanY",
        "zMean" => "MeanZ",
        "xStd" => "StdX",
        "yStd" => "StdY",
        "zStd" => "StdZ",
        "xAad" => "AverageAbsoluteDifferenceX",
        "yAad" => "AverageAbsoluteDifferenceY",
        "zAad" => "AverageAbsoluteDifferenceZ",
        "xMin" => "MinX",
        "yMin" => "MinY",
        "zMin" => "MinZ",
        "xMax" => "MaxX",
        "yMax" => "MaxY",
        "zMax" => "MaxZ",
        "xMaxMinDiff" => "MaxMinDifferenceX",
        "yMaxMinDiff" => "MaxMinDifferenceY",
        "zMaxMinDiff" => "MaxMinDifferenceZ",
        "xMedian" => "MedianX",
        "yMedian" => "MedianY",
        "zMedian" => "MedianZ",
        "xMad" => "MadX",
        "yMad" => "MadY",
        "zMad" => "MadZ",
        "xIqr" => "InterQuartileRangeX",
        "yIqr" => "InterQuartileRangeY",
        "zIqr" => "InterQuartileRangeZ",
        "xNegCount" => "CountNegativeX",
        "yNegCount" => "CountNegativeY",
        "zNegCount" => "CountNegativeZ",
        "xPosCount" => "CountPositiveX",
        "yPosCount" => "CountPositiveY",
        "zPosCount" => "CountPositiveZ",
        "xAboveMean" => "CountAboveMeanX",
        "yAboveMean" => "CountAboveMeanY",
        "zAboveMean" => "CountAboveMeanZ",
        "xEnergy" => "EnergyX",
        "yEnergy" => "EnergyY",
        "zEnergy" => "EnergyZ",
        "avgResultAcceleration" => "MeanAcceleration",
        "signalMagnitudeArea" => "SignalMagnitudeArea"
    )
end

function process(df, ::Type{MPathAmbientLight})
    safe_rename(
        df,
        "meanLux" => "MeanLux",
        "stdLux" => "StdLux",
        "minLux" => "MinLux",
        "maxLux" => "MaxLux"
    )
end

function process(df, ::Type{MPathAppUsage})
    @chain df begin
        safe_rename(
            "start" => "AppUsageStart",
            "end" => "AppUsageEnd",
            "usage" => "AppUsage"
        )
        transform(:AppUsage => ByRow(x -> collect(values(x))); renamecols = false)
        flatten(:AppUsage)
        transform(:AppUsage => AsTable)
        safe_rename(
            "packageName" => "AppName",
            "appName" => "AppShortName",
            "usage" => "AppUsageDuration",
            "startDate" => "AppStart",
            "endDate" => "AppEnd",
            "lastForeground" => "AppLastForeground"
        )
        transform(
            :AppUsageDuration => ByRow(x -> x / 1000000),
            [:AppUsageStart, :AppUsageEnd, :AppStart, :AppEnd, :AppLastForeground] .=>
                ByRow(x -> DateTime(x[1:23]));
            renamecols = false
        )
    end
end

function process(df, ::Type{MPathBattery})
    @chain df begin
        safe_rename(
            "batteryLevel" => "BatteryLevel",
            "batteryStatus" => "BatteryCharging"
        )
        transform(:BatteryCharging => ByRow(isequal("charging")); renamecols = false)
    end
end

function process(df, ::Type{MPathConnectivity})
    safe_rename(df, "connectivityStatus" => "ConnectivityStatus")
end

function process(df, ::Type{MPathDeviceInformation})
    safe_rename(
        df,
        "platform" => "Platform",
        "deviceId" => "DeviceID",
        "hardware" => "Hardware",
        "deviceName" => "DeviceName",
        "deviceManufacturer" => "DeviceManufacturer",
        "deviceModel" => "DeviceModel",
        "operatingSystem" => "OperatingSystem"
    )
end

function process(df, ::Type{MPathDisplay})
    @chain df begin
        safe_rename("screenEvent" => "ScreenEvent")
        transform(:ScreenEvent => ByRow(isequal("SCREEN_ON")) => :DisplayOn)
    end
end

function process(df, ::Type{MPathLocation})
    @chain df begin
        safe_rename(
            "latitude" => "Latitude",
            "longitude" => "Longitude",
            "altitude" => "Altitude",
            "accuracy" => "LocationAccuracy",
            "verticalAccuracy" => "VerticalAccuracy",
            "speed" => "Speed",
            "speedAccuracy" => "SpeedAccuracy",
            "heading" => "Heading",
            "time" => "LocationDateTime",
            "isMock" => "IsMock"
        )
        transform(:LocationDateTime => ByRow(DateTime); renamecols = false)
    end
end

function process(df, ::Type{MPathPhysicalActivity})
    @chain df begin
        safe_rename(
            "type" => "PhysicalActivityType",
            "confidence" => "PhysicalActivityConfidence"
        )
        transform(
            :PhysicalActivityType => (x -> replace(x,
                "IN_VEHICLE" => "InVehicle",
                "ON_BICYCLE" => "OnBicycle",
                "STILL" => "Still",
                "WALKING" => "Walking",
                "RUNNING" => "Running"
            ));
            renamecols = false
        )
    end
end

function process(df, ::Type{MPathMemory})
    safe_rename(
        df,
        "freePhysicalMemory" => "FreePhysicalMemory",
        "freeVirtualMemory" => "FreeVirtualMemory"
    )
end

process(df, ::Type{MPathSteps}) = safe_rename(df, "steps" => "CumulativeSteps")

process(df, ::Type{MPathTimeZone}) = safe_rename(df, "timezone" => "TimeZone")

function process(df, ::Type{MPathWeather})
    @chain df begin
        safe_rename(
            "country" => "Country",
            "areaName" => "AreaName",
            "weatherMain" => "WeatherMain",
            "weatherDescription" => "WeatherDescription",
            "date" => "WeatherDateTime",
            "sunrise" => "Sunrise",
            "sunset" => "Sunset",
            "latitude" => "Latitude",
            "longitude" => "Longitude",
            "pressure" => "Pressure",
            "windSpeed" => "WindSpeed",
            "windDegree" => "WindDegree",
            "humidity" => "Humidity",
            "cloudiness" => "Cloudiness",
            "temperature" => "Temperature",
            "tempMin" => "MinTemperature",
            "tempMax" => "MaxTemperature"
        )
        transform(
            [:WeatherDateTime, :Sunrise, :Sunset] .=> ByRow(DateTime);
            renamecols = false
        )
    end
end

process(df, ::Type{MPathWiFi}) = safe_rename(df, "ip" => "WiFiIPAddress")

####################################################################################################
# HELPER FUNCTIONS
####################################################################################################

function _mpath_connection_id(path)
    pieces = @chain path begin
        basename
        split("_"; keepempty = true)
    end

    if length(pieces) >= 3
        return convert(String, pieces[3])
    else
        return missing
    end
end

function _mpath_load(source, connectionid, ::Type{T}) where {T <: MPathMobileSensing}
    json = read(source, String)

    # if the json is not valid, attempt to fix it
    if !JSON.isvalidjson(json)
        length(json) <= 3 && return DataFrame((name => [] for name in variablenames(T))...)

        if last(json, 3) == ",\n]"
            json = json[1:(end - 3)] * "\n]"
        elseif last(json) == ","
            json = json[1:(end - 1)] * "]"
        elseif last(json, 2) == "}}"
            json = json * "]"
        end

        if !JSON.isvalidjson(json)
            return DataFrame((name => [] for name in variablenames(T))...)
        end
    end

    entries = @chain json begin
        JSON.parse
        filter(x -> haskey(x["data"], "__type") && x["data"]["__type"] == typename(T), _)
    end

    if isempty(entries)
        return DataFrame((name => [] for name in variablenames(T))...)
    else
        return @chain entries begin
            DataFrame

            # ensure that all entries have the same keys, so AsTable does not throw an error
            subset(:data => (x -> length.(keys.(x)) .== maximum(length.(keys.(x)))))

            transform(
                All() => ((x...) -> connectionid) => :MPathConnectionID,
                :sensorStartTime => (x -> unix2datetime.(x ./ 1000000)) => :DateTime,
                :data => AsTable
            )
            process(T)
            safe_select(variablenames(T))
        end
    end
end

####################################################################################################
# HIGH-LEVEL FUNCTIONS
####################################################################################################

"""
    gather(path, T) where {T <: MPathMobileSensing} -> DataFrame

Recursively read and process all mobile sensing data of type `T` in `path` and, if applicable,
the subfolders of `path`.
"""
function gather(path::AbstractString, ::Type{T}; args...) where {T <: MPathMobileSensing}
    if isdir(path)
        paths = readdir(path; join = true)

        if !isempty(paths)
            # recursively go through sub-directories and zip files and concatenate the results
            return vcat((gather(x, T; args...) for x in paths)...; cols = :union)
        end
    elseif iszipfile(path)
        return gather(read(path), T; args...)
    elseif endswith(path, ".json")
        return load(path, T)
    else
        return DataFrame((name => [] for name in variablenames(T))...)
    end
end

function gather(archive::ZipReader, ::Type{T}; args...) where {T <: MPathMobileSensing}
    jsonfiles = filter(endswith(".json"), zip_names(archive))

    if !isempty(jsonfiles)
        return vcat(
            (_mpath_load(
                 IOBuffer(zip_readentry(archive, x)), _mpath_connection_id(x), T; args...) for x in jsonfiles)...;
            cols = :union
        )
    else
        return DataFrame((name => [] for name in variablenames(T))...)
    end
end

function gather(dict::Dict{String, IO}, ::Type{T}; args...) where {T <: MPathMobileSensing}
    filenames = @chain dict begin
        keys
        filter(x -> endswith(x, ".zip") || endswith(x, ".json"), _)
        filter(!startswith("__"), _)
    end

    return vcat((endswith(x, ".zip") ? gather(dict[x], T; args...) :
                 _mpath_load(dict[x], _mpath_connection_id(x), T) for x in filenames)...)
end