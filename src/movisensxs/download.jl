
# 1. Exports
# 2. Generic Definitions
# 3. Helper Functions
# 4. Concrete Implementations

# Before you can use the API, you have to create an API key (admin rights needed):
# https://xs.movisens.com/administration/apikeys

####################################################################################################
# EXPORTS
####################################################################################################

export MovisensXS, MovisensXSResults, MovisensXSProbands, MovisensXSUnisens

####################################################################################################
# GENERIC DEFINITIONS
####################################################################################################

abstract type MovisensXS end

const MOVISENSXS_URL = "https://xs.movisens.com/api/v2/studies/"

####################################################################################################
# HELPER FUNCTIONS
####################################################################################################

function _movisensxs_download(url, apikey; headers = [], status_exception = true)
    response = HTTP.get(
        url,
        [
            "Authorization" => "ApiKey " * apikey,
            "User-Agent" => "AmbulatoryAssessmentAnalysis.jl",
            headers...
        ];
        status_exception
    )

    if response.status == 200
        return response.body
    else
        return nothing
    end
end

####################################################################################################
# CONCRETE IMPLEMENTATIONS
####################################################################################################

struct MovisensXSResults <: MovisensXS end
struct MovisensXSProbands <: MovisensXS end
struct MovisensXSUnisens <: MovisensXS end

"""
    download(::Type{MovisensXSResults}, studyid, apikey) -> DataFrame

Download e-diary data and return it as a `DataFrame`.
"""
function Base.download(
        ::Type{MovisensXSResults}, studyid::Union{AbstractString, Int}, apikey::AbstractString)
    @chain begin
        _movisensxs_download(MOVISENSXS_URL * string(studyid) * "/results", apikey;
            headers = ["Accept" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"])
        IOBuffer
        XLSX.readtable(1)
        DataFrame
    end
end

"""
    download(::Type{MovisensXSProbands}, studyid, apikey) -> Vector{Dict{String,Any}}

Download information about the participants.

The returned vector contains a dict for each participant, which may (but does not have to) contain keys such as
"id", "status", "currentVersion", "startDate", "endDate" and "coupleURL".
"""
function Base.download(
        ::Type{MovisensXSProbands}, studyid::Union{AbstractString, Int}, apikey::AbstractString)
    @chain begin
        _movisensxs_download(MOVISENSXS_URL * string(studyid) * "/probands", apikey)
        String
        JSON.parse
    end
end

"""
    download(::Type{MovisensXSUnisens}, studyid, participantid, apikey; status_exception = true) -> Vector{UInt8}

Download the mobile sensing data of a single participant.

Returns a `Vector{UInt8}` which represents a zip file.
"""
function Base.download(
        ::Type{MovisensXSUnisens}, studyid::Union{AbstractString, Int}, participantid::Union{
            AbstractString, Int},
        apikey::AbstractString, ; status_exception = true)
    _movisensxs_download(
        MOVISENSXS_URL * string(studyid) * "/probands/" * string(participantid) *
        "/unisens", apikey; status_exception)
end

"""
    download(dir, ::Type{MovisensXSUnisens}, studyid, apikey) -> nothing

Download the mobile sensing data of all participants and save them as zip files in `dir`.
"""
function Base.download(
        dir::AbstractString, ::Type{MovisensXSUnisens},
        studyid::Union{AbstractString, Int}, apikey::AbstractString)
    participantids = @chain begin
        Base.download(MovisensXSProbands, studyid, apikey)
        getindex.("id")
    end

    @showprogress "Downloading movisensXS mobile sensing data..." for participantid in participantids
        result = Base.download(
            MovisensXSUnisens, studyid, participantid, apikey; status_exception = false)

        if !isnothing(result)
            write(joinpath(dir, string(participantid) * ".zip"), result)
        end
    end

    return nothing
end