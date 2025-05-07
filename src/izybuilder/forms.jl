
# 1. Exports
# 2. Implementations
# 3. Helper Functions

####################################################################################################
# EXPORTS
####################################################################################################

export IzyBuilder

####################################################################################################
# IMPLEMENTATIONS
####################################################################################################

abstract type IzyBuilder end

####################################################################################################
# HELPER FUNCTIONS
####################################################################################################

function _izybuilder_tryparse_bool(x)
    isbool = @chain x begin
        filter(!ismissing, _)
        all(i -> i in ["Checked", "Unchecked"], _)
    end

    return isbool ? x .== "Checked" : x
end

function _izybuilder_parse_session(session, participant, sex)
    start = @chain begin
        session["Data"]
        replace("M\xe4r" => "Mar", "Mrz" => "Mar", "Maer" => "Mar",
            "Mai" => "May", "Okt" => "Oct", "Dez" => "Dec")

        # https://docs.julialang.org/en/v1/stdlib/Dates/#Dates.DateFormat
        DateTime(dateformat"d u y H:M")
    end

    duration = session["TotalTime"]

    if session["TimerInterrupted"] == "false"
        controls = @chain session begin
            children
            filter(x -> !isempty(children(x)), _)
            children.(_)
            vcat(_...)
            unique(x -> x["Name"], _)
        end
    else
        controls = []
    end

    DataFrame(
        "Participant" => participant,
        "Sex" => sex,
        "FormStart" => start,
        "FormDuration" => duration,
        [control["Name"] => control["Data"] for control in controls]...
    )
end

"""
    load(source, IzyBuilder) -> DataFrame

Read and process the given IzyBuilder file or `IOBuffer` and return a `DataFrame`.
"""
function load(source, ::Type{IzyBuilder})
    xml = XML.read(source, XML.Node)
    index = findfirst(x -> nodetype(x) == XML.Element, children(xml))
    participant = xml[index][2]["Id"]
    sex = xml[index][2]["Sex"]
    sessions = filter(x -> tag(x) == "Session", children(xml[index]))

    # TODO: parse total time?
    @chain sessions begin
        vcat(
            (_izybuilder_parse_session(session, participant, sex) for session in _)...;
            cols = :union
        )
        transform(All() .=> _izybuilder_tryparse_bool; renamecols = false)
    end
end

"""
    gather(path, IzyBuilder) -> DataFrame

Recursively read and process all IzyBuilder data in `path` and, if applicable, the subfolders of `path`.
"""
function gather(path::AbstractString, ::Type{IzyBuilder})
    if !isdir(path)
        return load(path, IzyBuilder)
    else
        # recursively go through sub-directories and concatenate the results
        return @chain path begin
            readdir(; join = true)
            vcat((gather(x, IzyBuilder) for x in _)...; cols = :union)
            groupby([:Participant, :Sex, :FormStart])
            combine(All() .=> first; renamecols = false)
        end
    end
end