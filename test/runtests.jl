using AmbulatoryAssessmentAnalysis
using Test
using Aqua

@testset "AmbulatoryAssessmentAnalysis.jl" begin
    @testset "Code quality (Aqua.jl)" begin
        Aqua.test_all(AmbulatoryAssessmentAnalysis)
    end
    # Write your tests here.
end
