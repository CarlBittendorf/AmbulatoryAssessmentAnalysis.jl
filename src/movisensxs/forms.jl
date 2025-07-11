
# 1. Exports
# 2. Generic Definitions
# 3. Concrete Implementations
# 4. High-level Functions

####################################################################################################
# EXPORTS
####################################################################################################

export MovisensXSForms, MovisensXSHourlyStates, MovisensXSMedication,
       MovisensXSStaticLocations
export preprocess

####################################################################################################
# GENERIC DEFINITIONS
####################################################################################################

abstract type MovisensXSForms end

Base.parse(T::Type{<:MovisensXSForms}) = (args...) -> parse(T, args...)

####################################################################################################
# CONCRETE IMPLEMENTATIONS
####################################################################################################

struct MovisensXSHourlyStates <: MovisensXSForms end
struct MovisensXSMedication <: MovisensXSForms end
struct MovisensXSStaticLocations <: MovisensXSForms end

function _movisensxs_assign_datetimes(start)
    # an hour state array always contains the states (awake, lying in bed, asleep) from 0-24 hours
    # but only the entries up to the last full hour before the query belong to the current day
    # the remaining hours are from the previous day
    [DateTime(Date(start)) + Hour(i) - Day(Time(start) > Time(Hour(i)) ? 0 : 1)
     for i in 0:23]
end

"""
    parse(MovisensXSHourlyStates, states, starts)

Read the hour state array created when logging sleep.

For a sleep query, all full hours before the query are specified in the correct chronological order,
followed by the last hours of the previous day. For example, if the query was answered at 9:30 p.m.,
the vector contains the information for the 21 hours of the current day, but the last three elements
are the hours from 9 p.m. to midnight of the previous day. This function corrects this
so that the labels in the vector correspond to the 24 hours of the day of the respective query.
If there are different entries for an hour, it is set to missing.

If the dataframe contains multiple participants, you should group by participant
before using this function.

# Examples

```julia
@chain df begin
    groupby(:MovisensXSParticipantID)
    transform([:HourlyStates, :FormStart] => parse(MovisensXSHourlyStates) => :HourlyStates)
end
```
"""
function Base.parse(::Type{MovisensXSHourlyStates}, states, starts)
    # Vector{Union{Vector{Int},Missing}}
    state_arrays = map(x -> ismissing(x) ? x : JSON.parse(x)["hourStateArray"], states)

    # Vector{Union{Vector{DateTime},Missing}}
    datetime_arrays = [ismissing(x) ? x : _movisensxs_assign_datetimes(start)
                       for (x, start) in zip(state_arrays, starts)]

    unique_datetimes = @chain starts begin
        skipmissing
        [[DateTime(date) + Hour(i) for i in 0:23] for date in Date.(_)]
        vcat(_...)
        unique
    end

    @chain begin
        DataFrame(
            :DateTime => vcat(filter(!ismissing, datetime_arrays)...),
            :HourlyState => vcat(filter(!ismissing, state_arrays)...)
        )

        # if a participant has provided different information about the same hour, set it to missing
        groupby(:DateTime)
        combine(:HourlyState => (x -> !isempty(x) && allequal(x) ? first(x) : missing) => :HourlyState)

        # add rows with missing for hours that were not specified
        leftjoin(DataFrame(:DateTime => unique_datetimes), _; on = :DateTime)

        sort(:DateTime)
        transform(
            :DateTime => ByRow(Date) => :Date,
            :HourlyState => (x -> replace(x,
                0 => "Awake",
                1 => "LyingInBed",
                2 => "Asleep"
            ));
            renamecols = false
        )

        # group all 24 hours of a day into one vector
        groupby(:Date)
        combine(:HourlyState => Ref => :HourlyStates)

        rightjoin(
            DataFrame(:Date => map(x -> ismissing(x) ? x : Date(x), starts));
            on = :Date,
            matchmissing = :equal,
            order = :right
        )
        getproperty(:HourlyStates)
    end
end

"""
    parse(MovisensXSMedication, medication)

Read the entries created when logging medication.

# Examples

```julia
transform(df, :Medication => parse(MovisensXSMedication) =>
            [:MedicationNames, :MedicationDoses, :MedicationAmounts, :MedicationTypes])
```
"""
function Base.parse(::Type{MovisensXSMedication}, medication::Union{String, Missing})
    ismissing(medication) && return missing

    # remove medications that were not taken
    dicts = filter(x -> x["taken"] != 0, JSON.parse(medication))

    return (
        [x["name"] for x in dicts],
        [x["dose"] for x in dicts],
        [x["taken"] for x in dicts],
        [x["type"] for x in dicts]
    )
end

function Base.parse(::Type{MovisensXSMedication}, medication)
    names = repeat(Union{Vector{String}, Missing}[missing], length(medication))
    doses = repeat(Union{Vector{String}, Missing}[missing], length(medication))
    amounts = repeat(Union{Vector{Int}, Missing}[missing], length(medication))
    types = repeat(Union{Vector{Int}, Missing}[missing], length(medication))

    for (i, entries) in enumerate(medication)
        ismissing(entries) && continue

        names[i], doses[i], amounts[i], types[i] = parse(MovisensXSMedication, entries)
    end

    return [names doses amounts types]
end

"""
    parse(MovisensXSStaticLocations, locations)

Read the entries created when logging static locations.

# Examples

```julia
transform(df, :StaticLocations => parse(MovisensXSStaticLocations) =>
            [:StaticLocationNames, :StaticLocationDateTimes, :StaticLatitudes,
            :StaticLongitudes, :StaticLocationConfidences, :StaticLocationRadii])
```
"""
function Base.parse(::Type{MovisensXSStaticLocations}, locations::Union{String, Missing})
    ismissing(locations) && return missing

    dicts = JSON.parse(locations)

    return (
        [x["name"] for x in dicts],
        [unix2datetime(x["timestamp"] / 1000) for x in dicts],
        [x["latitude"] for x in dicts],
        [x["longitude"] for x in dicts],
        [x["accuracy"] for x in dicts],
        [x["radius"] for x in dicts]
    )
end

function Base.parse(::Type{MovisensXSStaticLocations}, locations)
    names = repeat(Union{Vector{String}, Missing}[missing], length(locations))
    datetimes = repeat(Union{Vector{DateTime}, Missing}[missing], length(locations))
    latitudes = repeat(Union{Vector{Float64}, Missing}[missing], length(locations))
    longitudes = repeat(Union{Vector{Float64}, Missing}[missing], length(locations))
    confidences = repeat(Union{Vector{Float64}, Missing}[missing], length(locations))
    radii = repeat(Union{Vector{Int}, Missing}[missing], length(locations))

    for (i, entries) in enumerate(locations)
        ismissing(entries) && continue

        names[i], datetimes[i], latitudes[i], longitudes[i], confidences[i], radii[i] = parse(
            MovisensXSStaticLocations, entries)
    end

    return [names datetimes latitudes longitudes confidences radii]
end

####################################################################################################
# HIGH-LEVEL FUNCTIONS
####################################################################################################

function preprocess(df::DataFrame, ::Type{MovisensXSForms})
    @chain df begin
        transform(
            :Participant => ByRow(x -> round(Int, x)),
            :Form => ByRow(x -> x == "Missing") => :IsMissing;
            renamecols = false
        )
        rename(
            :Participant => :MovisensXSParticipantID,
            :Trigger_date => :FormTrigger,
            :Form_start_date => :FormStart,
            :Form_finish_date => :FormFinish,
            :Form_upload_date => :FormUpload,
            :Missing => :ReasonForMissing
        )
        select(Not(:Trigger_time, :Form_start_time, :Form_finish_time, :Form_upload_time))

        groupby([:MovisensXSParticipantID, :Trigger_counter])
        transform(
            :FormStart => minimum,
            :FormFinish => maximum;
            renamecols = false, ungroup = false
        )
        combine(All() .=> (x -> coalesce(x...)); renamecols = false)
    end
end

function aggregate(df::DataFrame, ::Type{MovisensXSHourlyStates}, period::Period;
        timecol = :FormTrigger, groupcols = [], statescol = :HourlyStates)
    @chain df begin
        flatten(statescol)

        groupby([groupcols..., timecol])
        transform(timecol => (x -> [floor(first(x), Day) + Hour(i) for i in 0:23]) => :DateTime)

        groupby_period(period; groupcols)
        combine(
            statescol => (x -> all(ismissing, x) ? missing : count(isequal("Asleep"), skipmissing(x))) => :HoursAsleep,
            statescol => (x -> all(ismissing, x) ? missing : count(isequal("LyingInBed"), skipmissing(x))) => :HoursLyingInBed,
            statescol => (x -> all(ismissing, x) ? missing : count(isequal("Awake"), skipmissing(x))) => :HoursAwake,
            statescol => (x -> all(ismissing, x) ? missing : count_changes(collect(skipmissing(x)))) => :StateChanges
        )
    end
end