
# 1. Exports
# 2. Generic Definitions
# 3. Concrete Implementations
# 4. Helper Functions
# 5. High-level Functions

# Further information: https://docs.movisens.com/movisensXS/mobile_sensing

####################################################################################################
# EXPORTS
####################################################################################################

export MovisensXSMobileSensing, MovisensXSAppUsage, MovisensXSBattery, MovisensXSCalls,
       MovisensXSDeviceRunning, MovisensXSDisplay, MovisensXSKeyboardInput,
       MovisensXSLocation, MovisensXSMusic, MovisensXSNearbyDevices,
       MovisensXSNotifications, MovisensXSPhysicalActivity, MovisensXSRecordedAudio,
       MovisensXSSMS, MovisensXSSteps, MovisensXSTraffic
export filenames, variablenames

####################################################################################################
# GENERIC DEFINITIONS
####################################################################################################

abstract type MovisensXSMobileSensing end

"""
    filenames(::Type{<:MovisensXSMobileSensing}) -> Vector{String}

Return the names of the files that are associated with this type of mobile sensing data.

# Examples

```jldoctest
julia> filenames(MovisensXSAppUsage)
1-element Vector{String}:
 "AppUsage.csv"

julia> filenames(MovisensXSTraffic)
2-element Vector{String}:
 "TrafficRx.csv"
 "TrafficTx.csv"
```
"""
function filenames end

"""
    variablenames(::Type{<:MovisensXSMobileSensing}) -> Vector{String}

Return the names of the variables belonging to this type of mobile sensing data.

# Examples

```jldoctest
julia> variablenames(MovisensXSAppUsage)
5-element Vector{String}:
 "MovisensXSParticipantID"
 "MovisensXSStudyID"
 "DateTime"
 "AppName"
 "AppAction"

julia> variablenames(MovisensXSDisplay)
4-element Vector{String}:
 "MovisensXSParticipantID"
 "MovisensXSStudyID"
 "DateTime"
 "DisplayOn"
```
"""
function variablenames end

"""
    load(source, ::Type{<:MovisensXSMobileSensing}) -> DataFrame

Read and process the given mobile sensing file or `IOBuffer` and return a `DataFrame`.

The returned `DataFrame` must contain a column "SecondsSinceStart".
"""
function load end

load(T::Type{<:MovisensXSMobileSensing}) = Base.Fix2(load, T)

####################################################################################################
# CONCRETE IMPLEMENTATIONS
####################################################################################################

struct MovisensXSAppUsage <: MovisensXSMobileSensing end
struct MovisensXSBattery <: MovisensXSMobileSensing end
struct MovisensXSCalls <: MovisensXSMobileSensing end
struct MovisensXSDeviceRunning <: MovisensXSMobileSensing end
struct MovisensXSDisplay <: MovisensXSMobileSensing end
struct MovisensXSKeyboardInput <: MovisensXSMobileSensing end
struct MovisensXSLocation <: MovisensXSMobileSensing end
struct MovisensXSMusic <: MovisensXSMobileSensing end
struct MovisensXSNearbyDevices <: MovisensXSMobileSensing end
struct MovisensXSNotifications <: MovisensXSMobileSensing end
struct MovisensXSPhysicalActivity <: MovisensXSMobileSensing end
struct MovisensXSRecordedAudio <: MovisensXSMobileSensing end
struct MovisensXSSMS <: MovisensXSMobileSensing end
struct MovisensXSSteps <: MovisensXSMobileSensing end
struct MovisensXSTraffic <: MovisensXSMobileSensing end

filenames(::Type{MovisensXSAppUsage}) = ["AppUsage.csv"]
filenames(::Type{MovisensXSBattery}) = ["BatteryLevel.csv"]
filenames(::Type{MovisensXSCalls}) = ["PhoneCallActivity.csv"]
filenames(::Type{MovisensXSDeviceRunning}) = ["DeviceRunning.csv"]
filenames(::Type{MovisensXSDisplay}) = ["DiplayOn.csv", "DisplayOn.csv"]
filenames(::Type{MovisensXSKeyboardInput}) = ["keyboard_input.csv"]
filenames(::Type{MovisensXSLocation}) = ["Location.csv"]
filenames(::Type{MovisensXSMusic}) = ["MusicLog.csv"]
filenames(::Type{MovisensXSNearbyDevices}) = ["Beacon.csv"]
filenames(::Type{MovisensXSNotifications}) = ["NotificationLog.csv"]
filenames(::Type{MovisensXSPhysicalActivity}) = ["ActivityLog.csv"]
filenames(::Type{MovisensXSRecordedAudio}) = ["RecordedAudio.csv"]
filenames(::Type{MovisensXSSMS}) = ["SMS.csv"]
filenames(::Type{MovisensXSSteps}) = ["Steps.csv"]
filenames(::Type{MovisensXSTraffic}) = ["TrafficRx.csv", "TrafficTx.csv"]

# define methods via metaprogramming to avoid repetitive code
for (type, names) in [
    MovisensXSAppUsage => ["AppName", "AppAction"],
    MovisensXSBattery => ["BatteryLevel"],
    MovisensXSCalls => ["CallType", "PartnerHash", "CallDuration"],
    MovisensXSDeviceRunning => ["DeviceRunning"],
    MovisensXSDisplay => ["DisplayOn"],
    MovisensXSKeyboardInput => [
        "KeyboardInputType", "KeyboardInputLengthPre", "KeyboardInputLengthPost"],
    MovisensXSLocation => ["Latitude", "Longitude", "Altitude", "LocationConfidence"],
    MovisensXSMusic => ["MusicOn", "Artist", "Album", "Track"],
    MovisensXSNearbyDevices => [
        "BeaconEvent", "ProximityUUID", "MajorValue", "MinorValue", "BeaconURL"],
    MovisensXSNotifications => ["AppName", "NotificationLength"],
    MovisensXSPhysicalActivity => ["PhysicalActivityType", "PhysicalActivityConfidence"],
    MovisensXSRecordedAudio => ["RecordedAudioEvent", "RecordedAudioFilename"],
    MovisensXSSMS => ["SMSType", "PartnerHash", "SMSLength"],
    MovisensXSSteps => ["Steps"],
    MovisensXSTraffic => [
        "ReceivedAppTraffic", "ReceivedMobileTraffic", "ReceivedTotalTraffic",
        "TransmittedAppTraffic", "TransmittedMobileTraffic", "TransmittedTotalTraffic"]
]
    # MovisensXSParticipantID, MovisensXSStudyID, DateTime are always the first three columns
    pushfirst!(names, "MovisensXSParticipantID", "MovisensXSStudyID", "DateTime")

    @eval variablenames(::Type{$type}) = $names
end

function load(source, ::Type{MovisensXSAppUsage})
    @chain source begin
        read(String)
        replace("/" => ",")
        IOBuffer
        CSV.read(DataFrame; select = [1, 3, 4], header = false)
        safe_rename(["SecondsSinceStart", "AppName", "AppAction"])
    end
end

function load(source, ::Type{MovisensXSCalls})
    @chain source begin
        read(String)
        replace("=" => ",", "|" => ",", "\"" => ",")
        IOBuffer
        CSV.read(DataFrame; select = [1, 4, 9, 12], header = false)
        safe_rename(["SecondsSinceStart", "CallType", "PartnerHash", "CallDuration"])
        dropmissing(:CallType)
    end
end

function load(source, ::Type{MovisensXSDeviceRunning})
    @chain source begin
        CSV.read(DataFrame; header = false)
        safe_rename(["SecondsSinceStart", "DeviceRunning"])
        transform(:DeviceRunning => ByRow(isequal(1)); renamecols = false)
    end
end

function load(sources::AbstractVector, ::Type{MovisensXSDisplay})
    @chain sources begin
        vcat((CSV.read(source, DataFrame; header = false) for source in _)...)
        safe_rename(["SecondsSinceStart", "DisplayOn"])
        transform(:DisplayOn => ByRow(isequal(1)); renamecols = false)
    end
end

load(source, ::Type{MovisensXSDisplay}) = load([source], MovisensXSDisplay)

function load(source, ::Type{MovisensXSKeyboardInput})
    @chain source begin
        read(String)
        replace(";" => ",")
        IOBuffer
        CSV.read(DataFrame; header = false, silencewarnings = true)
        safe_rename(["MillisecondsSinceStart", "KeyboardInputType",
            "KeyboardInputLengthPre", "KeyboardInputLengthPost"])
        transform(
            :MillisecondsSinceStart => ByRow(x -> x / 1000) => :SecondsSinceStart,
            :KeyboardInputType => (x -> replace(x,
                "B" => "BackSpace",
                "S" => "Space",
                "C" => "Character",
                "AC" => "AutoCorrect"
            ));
            renamecols = false
        )
    end
end

function load(source, ::Type{MovisensXSMusic})
    @chain source begin
        read(String)
        replace("=" => ",", "|" => ",", "\"" => ",")
        IOBuffer
        CSV.read(
            DataFrame;
            select = [1, 4, 6, 8, 10], header = false, silencewarnings = true
        )
        safe_rename(["SecondsSinceStart", "MusicOn", "Artist", "Album", "Track"])
    end
end

function load(source, ::Type{MovisensXSNearbyDevices})
    @chain source begin
        read(String)
        replace("=" => ",", "|" => ",", "\"" => ",", " " => ",")
        IOBuffer
        CSV.read(
            DataFrame;
            select = [1, 3, 5, 6, 7, 9], header = false, silencewarnings = true
        )
        safe_rename(["SecondsSinceStart", "BeaconEvent",
            "ProximityUUID", "Type", "Value", "MinorValue"])
        transform(
            :BeaconEvent => ByRow(uppercasefirst),
            [:Type, :Value] => ByRow((t, x) -> t == "major" ? [x, missing] : [missing, x]) => [
                :MajorValue, :BeaconURL];
            renamecols = false
        )
        transform(
            :MajorValue => ByRow(x -> !ismissing(x) && !(x isa Int) ? tryparse(Int, x) : x);
            renamecols = false
        )
    end
end

function load(source, ::Type{MovisensXSNotifications})
    @chain source begin
        read(String)
        replace("=" => ",", "|" => ",", "\"" => ",")
        IOBuffer
        CSV.read(DataFrame; select = [1, 4, 6], header = false)
        safe_rename(["SecondsSinceStart", "AppName", "NotificationLength"])
    end
end

function load(source, ::Type{MovisensXSPhysicalActivity})
    @chain source begin
        CSV.read(DataFrame; header = false)
        safe_rename([
            "SecondsSinceStart", "PhysicalActivityType", "PhysicalActivityConfidence"])
        transform(
            :PhysicalActivityType => (x -> replace(x,
                0 => "InVehicle",
                1 => "OnBicycle",
                2 => "OnFoot",
                3 => "Still",
                4 => "Unknown",
                5 => "Tilting"
            ));
            renamecols = false
        )
    end
end

function load(source, ::Type{MovisensXSRecordedAudio})
    @chain source begin
        CSV.read(DataFrame; header = false)
        safe_rename(["SecondsSinceStart", "RecordedAudioEvent", "RecordedAudioFilename"])
        transform(
            :RecordedAudioEvent => ByRow(x -> uppercasefirst(last(split(x, " "))));
            renamecols = false
        )
    end
end

function load(source, ::Type{MovisensXSSMS})
    @chain source begin
        read(String)
        replace("=" => ",", "|" => ",", "\"" => ",")
        IOBuffer
        CSV.read(DataFrame; select = [1, 4, 9, 12], header = false)
        safe_rename(["SecondsSinceStart", "SMSType", "PartnerHash", "SMSLength"])
    end
end

function load(sources::Vector{T}, ::Type{MovisensXSTraffic}) where {T}
    df_rx = @chain sources[1] begin
        CSV.read(DataFrame; header = false)
        safe_rename(["SecondsSinceStart", "ReceivedAppTraffic",
            "ReceivedMobileTraffic", "ReceivedTotalTraffic"])
    end

    df_tx = @chain sources[2] begin
        CSV.read(DataFrame; header = false)
        safe_rename(["SecondsSinceStart", "TransmittedAppTraffic",
            "TransmittedMobileTraffic", "TransmittedTotalTraffic"])
    end

    return outerjoin(df_rx, df_tx; on = :SecondsSinceStart)
end

# define methods via metaprogramming to avoid repetitive code
for (type, names) in [
    MovisensXSBattery => ["BatteryLevel"],
    MovisensXSLocation => ["Latitude", "Longitude", "Altitude", "LocationConfidence"],
    MovisensXSSteps => ["Steps"]
]
    # SecondsSinceStart is always the first column
    pushfirst!(names, "SecondsSinceStart")

    @eval function load(source, ::Type{$type})
        @chain source begin
            CSV.read(DataFrame; header = false)
            safe_rename($names)
        end
    end
end

####################################################################################################
# HELPER FUNCTIONS
####################################################################################################

function _movisensxs_unisens_attribute(xml, key)
    # find the first child with the given attribute
    index = findfirst(x -> x["key"] == key, children(xml))

    # return its value
    return xml[index]["value"]
end

function _movisensxs_process(::Type{T}, sources, unisens;
        callback = (df, participantid, studyid) -> df) where {T <: MovisensXSMobileSensing}
    xml = XML.read(unisens, XML.Node)
    start = DateTime(xml[2]["timestampStart"])
    participantid = _movisensxs_unisens_attribute(xml[2][1], "probandId")
    studyid = _movisensxs_unisens_attribute(xml[2][1], "studyId")

    @chain sources begin
        load(T)
        callback(participantid, studyid)
        transform(
            All() => ((x...) -> participantid) => :MovisensXSParticipantID,
            All() => ((x...) -> studyid) => :MovisensXSStudyID,
            :SecondsSinceStart => ByRow(x -> start + Millisecond(round(Int, x * 1000))) => :DateTime
        )
        select(variablenames(T))
    end
end

function _movisensxs_gather(
        ::Type{T}, sources, unisens, names; args...) where {T <: MovisensXSMobileSensing}
    if isempty(sources)
        return DataFrame((x => [] for x in variablenames(T))...)
    elseif length(sources) == 1
        return _movisensxs_process(T, only(sources), unisens; args...)
    else
        # sort the sources by their order in filenames
        indices = sort(
            eachindex(names); by = i -> findfirst(x -> endswith(names[i], x), filenames(T)))

        return _movisensxs_process(T, sources[indices], unisens; args...)
    end
end

####################################################################################################
# HIGH-LEVEL FUNCTIONS
####################################################################################################

"""
    gather(path, T; callback) where {T <: MovisensXSMobileSensing} -> DataFrame

Recursively read and process all mobile sensing data of type `T` in `path` and, if applicable,
the subfolders of `path`.

`callback` can be used, for example, to correct server errors and must be a function that takes
three arguments: A `DataFrame` (containing the column "SecondsSinceStart"), the participant ID and
the study ID. It should return a `DataFrame` with the same column names.
"""
function gather(
        path::AbstractString, ::Type{T}; args...) where {T <: MovisensXSMobileSensing}
    !isdir(path) && !iszipfile(path) && return load(path, T)

    if isdir(path)
        paths = readdir(path; join = true)
        index = findfirst(x -> endswith(x, "unisens.xml"), paths)

        if isnothing(index)
            filter!(x -> isdir(x) || iszipfile(x), paths)

            # recursively go through sub-directories and zip files and concatenate the results
            return vcat((gather(x, T; args...) for x in paths)...)
        else
            names = filter(path -> any(x -> endswith(path, x), filenames(T)), paths)

            return _movisensxs_gather(T, names, paths[index], names; args...)
        end
    else
        return gather(read(path), T; args...)
    end
end

function gather(archive::ZipReader, ::Type{T}; args...) where {T <: MovisensXSMobileSensing}
    index = findfirst(x -> endswith(x, "unisens.xml"), zip_names(archive))

    if isnothing(index)
        zipfiles = filter(endswith(x, ".zip"), zip_names(archive))

        return vcat((gather(zip_readentry(archive, x), T; args...) for x in zipfiles)...)
    else
        names = filter(
            name -> any(x -> endswith(name, x), filenames(T)), zip_names(archive))
        sources = [IOBuffer(zip_readentry(archive, name)) for name in names]
        unisens = IOBuffer(zip_readentry(archive, zip_names(archive)[index]))

        return _movisensxs_gather(T, sources, unisens, names; args...)
    end
end

gather(x::AbstractVector{UInt8}, T::Type; args...) = gather(ZipReader(x), T; args...)

gather(io::IO, T::Type; args...) = gather(read(io), T; args...)

function gather(dict::Dict{String, IO}, T::Type; args...)
    filenames = @chain dict begin
        keys
        filter(endswith(".zip"), _)
        filter(!startswith("__"), _)
    end

    return vcat((gather(dict[x], T; args...) for x in filenames)...)
end

"""
    aggregate(df, T, period) -> DataFrame

Aggregate mobile sensing data at the level of `period` (e.g. `Day(1)` or `Hour(6)`).

Here are the types of mobile sensing data and the calculated variables that currently have implementations:
* `MovisensXSCalls`: IncomingCalls, IncomingCalls, IncomingMissedCalls, IncomingMissedCalls,
SecondsCallDuration, UniqueConversationPartners
* `MovisensXSDisplay`: CountDisplayOn, SecondsDisplayOff, SecondsDisplayOn
* `MovisensXSPhysicalActivity`: SecondsInVehicle, SecondsOnBicycle, SecondsOnFoot, SecondsStill,
SecondsTilting, SecondsUnknown
* `MovisensXSSteps`: Steps
* `MovisensXSTraffic`: ReceivedAppTraffic, ReceivedMobileTraffic, ReceivedTotalTraffic,
TransmittedAppTraffic, TransmittedMobileTraffic, TransmittedTotalTraffic
"""
function aggregate(df::DataFrame, ::Type{MovisensXSCalls}, period::Period)
    @chain df begin
        groupby_period(period; groupcols = [:MovisensXSParticipantID, :MovisensXSStudyID])
        combine(
            nrow => :TotalCalls,
            :CallType => (x -> count(x .== "Incoming")) => :IncomingCalls,
            :CallType => (x -> count(x .== "Outgoing")) => :OutgoingCalls,
            :CallType => (x -> count(x .== "IncomingMissed")) => :IncomingMissedCalls,
            :CallType => (x -> count(x .== "OutgoingNotReached")) => :OutgoingNotReachedCalls,
            :CallDuration => sum => :SecondsCallDuration,
            :PartnerHash => (x -> length(unique(x))) => :UniqueConversationPartners
        )
    end
end

function aggregate(df::DataFrame, ::Type{MovisensXSDisplay}, period::Period)
    groupcols = [:MovisensXSParticipantID, :MovisensXSStudyID]

    @chain df begin
        insert_period_starts(period; groupcols)

        groupby(groupcols)
        transform(
            :DisplayOn => fill_down,
            :DateTime => duration_to_next(period) => :DisplayDuration;
            renamecols = false
        )

        groupby_period(period; groupcols)
        combine(
            :DisplayOn => count => :CountDisplayOn,
            [:DisplayOn, :DisplayDuration] => ((o, d) -> sum(d[.!o])) => :SecondsDisplayOff,
            [:DisplayOn, :DisplayDuration] => ((o, d) -> sum(d[o])) => :SecondsDisplayOn
        )
    end
end

function aggregate(
        df::DataFrame, ::Type{MovisensXSLocation}, period::Period;
        max_velocity = 300, threshold = 20
)
    groupcols = [:MovisensXSParticipantID, :MovisensXSStudyID]

    @chain df begin
        filter_locations(; max_velocity, groupcols)

        groupby_period(period; groupcols)
        combine(
            :Distance => sum => :KilometersTotal,
            [:Distance, :Velocity] => ((d, v) -> sum(d[v .< threshold])) => :KilometersSlow,
            [:Distance, :Velocity] => ((d, v) -> sum(d[v .>= threshold])) => :KilometersFast,
            :Velocity => length => :MinutesMovingTotal,
            :Velocity => (x -> count(x .< threshold)) => :MinutesMovingSlow,
            :Velocity => (x -> count(x .>= threshold)) => :MinutesMovingFast
        )
    end
end

function aggregate(df::DataFrame, ::Type{MovisensXSPhysicalActivity}, period::Period)
    groupcols = [:MovisensXSParticipantID, :MovisensXSStudyID]

    @chain df begin
        sort(:DateTime)
        groupby(groupcols)
        transform(:DateTime => duration_to_previous(period; maxduration = 60) => :PhysicalActivityDuration)

        groupby_period(period; groupcols)
        combine(
            [:PhysicalActivityDuration, :PhysicalActivityType] => ((d, t) -> sum(d[t .== "InVehicle"])) => :SecondsInVehicle,
            [:PhysicalActivityDuration, :PhysicalActivityType] => ((d, t) -> sum(d[t .== "OnBicycle"])) => :SecondsOnBicycle,
            [:PhysicalActivityDuration, :PhysicalActivityType] => ((d, t) -> sum(d[t .== "OnFoot"])) => :SecondsOnFoot,
            [:PhysicalActivityDuration, :PhysicalActivityType] => ((d, t) -> sum(d[t .== "Still"])) => :SecondsStill,
            [:PhysicalActivityDuration, :PhysicalActivityType] => ((d, t) -> sum(d[t .== "Tilting"])) => :SecondsTilting
        )

        # all remaining time is treated as unknown
        transform([:SecondsInVehicle, :SecondsOnBicycle, :SecondsOnFoot, :SecondsStill, :SecondsTilting]
        => ((x...) -> Dates.value(Second(period)) .- sum(x)) => :SecondsUnknown)
    end
end

function aggregate(df::DataFrame, ::Type{MovisensXSSteps}, period::Period)
    @chain df begin
        subset(:Steps => x -> x .>= 0)
        groupby_period(period; groupcols = [:MovisensXSParticipantID, :MovisensXSStudyID])
        combine(:Steps => sum => :Steps)
    end
end

function aggregate(df::DataFrame, ::Type{MovisensXSTraffic}, period::Period)
    groupcols = [:MovisensXSParticipantID, :MovisensXSStudyID]
    variables = [:ReceivedAppTraffic, :ReceivedMobileTraffic, :ReceivedTotalTraffic,
        :TransmittedAppTraffic, :TransmittedMobileTraffic, :TransmittedTotalTraffic]

    @chain df begin
        sort(:DateTime)
        groupby(groupcols)
        transform(variables .=> fill_down; renamecols = false, ungroup = false)

        # the variables contain bytes accumulated over several days, so calculate the increments
        transform(
            variables .=>
                (x -> ifelse.(
                    x .>= ShiftedArrays.lag(x; default = first(x)),
                    x .- ShiftedArrays.lag(x; default = first(x)),
                    x
                ));
            renamecols = false
        )

        groupby_period(period; groupcols)
        combine(variables .=> sum; renamecols = false)
    end
end