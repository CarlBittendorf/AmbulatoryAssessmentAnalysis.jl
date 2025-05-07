using AmbulatoryAssessmentAnalysis
using Test, Random, InteractiveUtils, Dates
using Aqua, DataFrames, JSON, Chain

@testset "AmbulatoryAssessmentAnalysis.jl" begin
    @testset "Code quality (Aqua.jl)" begin
        Aqua.test_all(AmbulatoryAssessmentAnalysis)
    end

    include("izybuilder/forms.jl")

    include("movisensxs/sensing.jl")
    include("movisensxs/forms.jl")

    include("open-meteo/download.jl")
end
