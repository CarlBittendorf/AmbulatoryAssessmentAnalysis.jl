
# 1. Exports
# 2. Implementations
# 3. Internal Functions

####################################################################################################
# EXPORTS
####################################################################################################

export fill_down, fill_up, enumerate_days, enumerate_clusters, chunk, duration_to_next,
       duration_to_previous, groupby_period, insert_period_starts, fill_periods,
       count_unique, count_changes, frequencies_of_occurrence, filter_locations

####################################################################################################
# IMPLEMENTATIONS
####################################################################################################

"""
    fill_down(x)

Replace missing values with the last previous non-missing value or, if none exists,
with the first non-missing value that occurs in `x`.

# Examples

```jldoctest
julia> fill_down([1, 2, missing, 4, missing])
5-element Vector{Int64}:
 1
 2
 2
 4
 4

julia> fill_down([missing, "x", missing, "y", missing])
5-element Vector{String}:
 "x"
 "x"
 "x"
 "y"
 "y"
```
"""
fill_down(x) = accumulate((a, b) -> coalesce(b, a), x; init = coalesce(x...))

"""
    fill_up(x; maxsteps = length(x) - 1)

Replace missing values with the next non-missing value (if it exists).

# Examples

```jldoctest
julia> fill_up([1, 2, missing, 4, missing])
5-element Vector{Union{Missing, Int64}}:
 1
 2
 4
 4
  missing

julia> fill_up([missing, missing, missing, missing, "x", "y"]; maxsteps = 3)
6-element Vector{Union{Missing, String}}:
 missing
 "x"
 "x"
 "x"
 "x"
 "y"
```
"""
function fill_up(x; maxsteps = length(x) - 1)
    map(i -> coalesce(x[i:min(i + maxsteps, length(x))]...), eachindex(x))
end

fill_up(; maxsteps = length(x) - 1) = x -> fill_up(x; maxsteps)

"""
    enumerate_days(x) -> Vector{Int}

Enumerate days starting with the earliest date (which receives a value of 1).

# Examples

```jldoctest
julia> using Dates

julia> enumerate_days(Date.(["2025-01-01", "2025-01-02", "2025-02-01", "2025-02-02"]))
4-element Vector{Int64}:
  1
  2
 32
 33

julia> enumerate_days(Date.(["2025-02-01", "2025-02-02", "2025-01-01", "2025-01-02"]))
4-element Vector{Int64}:
 32
 33
  1
  2
```
"""
enumerate_days(x) = Dates.value.(Date.(x) .- Date(minimum(x))) .+ 1

"""
    enumerate_clusters(x) -> Vector{Int}

Enumerate clustered points that are consecutive in time.

# Examples

```jldoctest
julia> enumerate_clusters([3, 3, 8, 5, 5, 5, 3, 8, 8])
9-element Vector{Int64}:
 1
 1
 2
 3
 3
 3
 4
 5
 5
```
"""
function enumerate_clusters(x)
    labels = ones(Int, length(x))

    for i in eachindex(x)
        i == 1 && continue

        if x[i] == x[i - 1]
            labels[i] = labels[i - 1]
        else
            labels[i] = labels[i - 1] + 1
        end
    end

    return labels
end

"""
    chunk(N, size) -> Vector{UnitRange{Int64}}

Return a vector of ranges that divide `N` elements into groups of maximum size `size`.

# Examples

```jldoctest
julia> chunk(100, 25)
4-element Vector{UnitRange{Int64}}:
 1:25
 26:50
 51:75
 76:100

julia> chunk(10, 4)
3-element Vector{UnitRange{Int64}}:
 1:4
 5:8
 9:10
```
"""
chunk(N::Int, size::Int) = [((i - 1) * size + 1):min(i * size, N)
                            for i in 1:ceil(Int, N / size)]

"""
    duration_to_next(x, period; maxduration = Inf)

For each element, calculate the duration (in seconds) until the next time point
or until the end of the current period, whichever is shorter.

See also [`duration_to_previous`](@ref).

# Examples

```jldoctest
julia> using Dates

julia> x = Date("2025-01-01") .+ Time.(["11:00:00", "11:10:30", "12:20:00", "12:45:00"])
4-element Vector{DateTime}:
 2025-01-01T11:00:00
 2025-01-01T11:10:30
 2025-01-01T12:20:00
 2025-01-01T12:45:00

julia> duration_to_next(x, Hour(1))
4-element Vector{Float64}:
  630.0
 2970.0
 1500.0
    0.0

julia> duration_to_next(x, Minute(30))
4-element Vector{Float64}:
  630.0
 1170.0
  600.0
    0.0
```
"""
function duration_to_next(x, period::Period; maxduration = Inf)
    # the next entry or period, which ever comes first
    # the last entry is assigned a duration of 0
    next = min.(
        ShiftedArrays.lead(x; default = last(x)),
        ifelse.(x .== ceil.(x, period), x .+ period, ceil.(x, period))
    )

    return min.(Dates.value.(next .- x) ./ 1000, maxduration)
end

function duration_to_next(period::Period; maxduration = Inf)
    x -> duration_to_next(x, period; maxduration)
end

"""
    duration_to_previous(x, period; maxduration = Inf)

For each element, calculate the duration (in seconds) since the previous time point
or since the start of the current period, whichever is shorter.

See also [`duration_to_next`](@ref).

# Examples

```jldoctest
julia> using Dates

julia> x = Date("2025-01-01") .+ Time.(["11:00:00", "11:10:30", "12:20:00", "12:45:00"])
4-element Vector{DateTime}:
 2025-01-01T11:00:00
 2025-01-01T11:10:30
 2025-01-01T12:20:00
 2025-01-01T12:45:00

julia> duration_to_previous(x, Hour(1))
4-element Vector{Float64}:
    0.0
  630.0
 1200.0
 1500.0

julia> duration_to_previous(x, Minute(10))
4-element Vector{Float64}:
   0.0
  30.0
 600.0
 300.0
```
"""
function duration_to_previous(x, period::Period; maxduration = Inf)
    # the previous entry or period, which ever comes last
    previous = max.(
        ShiftedArrays.lag(x; default = first(x)),
        ifelse.(x .== floor.(x, period), x .- period, floor.(x, period))
    )

    return min.(Dates.value.(x .- previous) ./ 1000, maxduration)
end

function duration_to_previous(period::Period; maxduration = Inf)
    x -> duration_to_previous(x, period; maxduration)
end

"""
    groupby_period(df, period; timecol = :DateTime, groupcols = []) -> GroupedDataFrame

Group the rows of a dataframe `df` according to the time interval in which they lie and `groupcols`.

The `timecol` column contains the timestamps.
"""
function groupby_period(df::DataFrame, period::Period; timecol = :DateTime, groupcols = [])
    @chain df begin
        transform(timecol => ByRow(x -> floor(x, period)); renamecols = false)
        groupby([groupcols..., timecol])
    end
end

"""
    insert_period_starts(df, period; timecol = :DateTime, groupcols = []) -> DataFrame

Insert an additional row at the beginning of each period that occurs in the dataframe,
except the very first.

The `timecol` column contains the timestamps. The groupcols argument should be used if,
for example, there are multiple participants in the dataframe.
"""
function insert_period_starts(
        df::DataFrame, period::Period; timecol = :DateTime, groupcols = [])
    df_extra = @chain df begin
        groupby_period(period; groupcols)
        combine(first)
        select([groupcols..., timecol])

        groupby(groupcols)
        subset(timecol => (x -> x .!= minimum(x)))
    end

    @chain df begin
        vcat(df_extra; cols = :union)
        sort([groupcols..., timecol])
    end
end

"""
    fill_periods(df, period, step; timecol = :DateTime, groupcols = []) -> DataFrame

Fill each `period` in the dataframe with rows spaced by `steps`, but do not add any entry that is before the first
or after the last timestamp of the respective participant.

The `timecol` column contains the timestamps. The groupcols argument should be used if,
for example, there are multiple participants in the dataframe.

# Examples

```jldoctest
julia> using DataFrames, Dates

julia> df = DataFrame(:DateTime => Date("2025-01-01") .+ Time.(["10:05:00", "12:45:00"]))
2×1 DataFrame
 Row │ DateTime            
     │ DateTime            
─────┼─────────────────────
   1 │ 2025-01-01T10:05:00
   2 │ 2025-01-01T12:45:00

julia> fill_periods(df, Hour(1), Minute(1))
101×1 DataFrame
 Row │ DateTime            
     │ DateTime            
─────┼─────────────────────
   1 │ 2025-01-01T10:05:00
   2 │ 2025-01-01T10:06:00
   3 │ 2025-01-01T10:07:00
  ⋮  │          ⋮
  99 │ 2025-01-01T12:43:00
 100 │ 2025-01-01T12:44:00
 101 │ 2025-01-01T12:45:00
            95 rows omitted
```
"""
function fill_periods(
        df::DataFrame, period::Period, step; timecol = :DateTime, groupcols = [])
    df_filled = @chain df begin
        groupby(groupcols)
        transform(
            timecol => minimum => :Start,
            timecol => maximum => :End
        )
        groupby_period(period; groupcols)
        combine([timecol, :Start, :End]
        => ((x, s, e) -> range(max(first(s), first(x)), min(first(e), first(x) + period - step); step)) => :DateTimeFilled)
        select(Not(timecol))
        rename(:DateTimeFilled => timecol)
    end

    @chain df begin
        outerjoin(df_filled; on = [groupcols..., timecol])
        sort([groupcols..., timecol])
    end
end

"""
    count_unique(x) -> Int

Count the unique number of elements in `x`.

# Examples

```jldoctest
julia> count_unique([3, 3, 8, 5, 5, 5, 3, 8, 8])
3

julia> count_unique(1:10)
10
```
"""
count_unique(x) = length(unique(x))

"""
    count_changes(x) -> Int

Count the number of consecutive elements that are different from each other.

# Examples

```jldoctest
julia> count_changes([3, 3, 8, 5, 5, 5, 3, 8, 8])
4

julia> count_changes(collect(1:10))
9
```
"""
count_changes(x) = count(x .!= ShiftedArrays.lag(x; default = first(x)))

"""
    frequencies_of_occurrence(x) -> Vector{Int}

Return the frequencies of occurrence for each unique element in `x`.

# Examples

```jldoctest
julia> frequencies_of_occurrence([3, 3, 8, 5, 5, 5, 3, 8, 8])
3-element Vector{Int64}:
 3
 3
 3
```
"""
function frequencies_of_occurrence(x)
    # this is equivalent to but ~5x faster than `[count(isequal(e), x) for e in unique(x)]`
    @chain DataFrame(X = x) begin
        groupby(:X)
        combine(nrow)
        getproperty(:nrow)
    end
end

function kilometers_to_previous(latitudes, longitudes)
    map(
        (u, v, x, y) -> haversine([u, v], [x, y]) / 1000,
        ShiftedArrays.lag(latitudes; default = first(latitudes)),
        ShiftedArrays.lag(longitudes; default = first(longitudes)),
        latitudes,
        longitudes
    )
end

function filter_locations(df::DataFrame; max_velocity, groupcols = [])
    @chain df begin
        # use only the first entry of each minute
        groupby_period(Minute(1); groupcols)
        combine(All() .=> first; renamecols = false)

        # calculate distances between consecutive entries
        groupby(groupcols)
        transform([:Latitude, :Longitude] => kilometers_to_previous => :Distance)

        # remove values implying velocities > max_velocity
        transform(:Distance => ByRow(x -> x * 60) => :Velocity)
        subset(:Velocity => (x -> x .<= max_velocity))
    end
end

####################################################################################################
# INTERNAL FUNCTIONS
####################################################################################################

"""
    iszipfile(x) -> Bool

Check whether a given path `x` is a zip file.
"""
iszipfile(x::AbstractString) = isfile(x) && endswith(x, ".zip")

"""
    safe_rename(df, names) -> DataFrame

Rename the columns of a dataframe or, if the dataframe has no columns, create a new dataframe
with the given column names and zero rows.
"""
function safe_rename(df::DataFrame, names)
    if ncol(df) == 0
        return DataFrame((name => [] for name in names)...)
    else
        return rename(df, names)
    end
end