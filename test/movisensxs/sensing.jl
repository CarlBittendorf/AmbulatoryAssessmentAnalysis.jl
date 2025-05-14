
@testset "movisensXS Mobile Sensing" begin
    for T in subtypes(MovisensXSMobileSensing)
        # zip files
        @test gather("movisensxs/data/1.zip", T) isa DataFrame
        @test names(gather("movisensxs/data/1.zip", T)) == variablenames(T)
        @test gather("movisensxs/data/2.zip", T) isa DataFrame
        @test names(gather("movisensxs/data/2.zip", T)) == variablenames(T)

        # zip files as Vector{UInt8} or IOBuffer
        @test gather(read("movisensxs/data/1.zip"), T) isa DataFrame
        @test names(gather(read("movisensxs/data/1.zip"), T)) == variablenames(T)
        @test gather(IOBuffer(read("movisensxs/data/1.zip")), T) isa DataFrame
        @test names(gather(IOBuffer(read("movisensxs/data/1.zip")), T)) == variablenames(T)

        # unisens folders
        @test gather("movisensxs/data/3", T) isa DataFrame
        @test names(gather("movisensxs/data/3", T)) == variablenames(T)
        @test gather("movisensxs/data/4", T) isa DataFrame
        @test names(gather("movisensxs/data/4", T)) == variablenames(T)

        # folder containing both zip files and unisens folders
        @test gather("movisensxs/data", T) isa DataFrame
        @test names(gather("movisensxs/data", T)) == variablenames(T)

        # Dict{String,IO}
        dict = Dict{String, IO}(
            "1.zip" => IOBuffer(read("movisensxs/data/1.zip")),
            "2.zip" => IOBuffer(read("movisensxs/data/2.zip")),
            "test.txt" => IOBuffer(b"AmbulatoryAssessmentAnalysis.jl"),
            "__MACOSX/1.zip" => IOBuffer(b"AmbulatoryAssessmentAnalysis.jl")
        )
        @test names(gather(dict, T)) == variablenames(T)
    end

    for T in (
        MovisensXSCalls, MovisensXSDisplay, MovisensXSLocation, MovisensXSSteps,
        MovisensXSPhysicalActivity, MovisensXSTraffic
    )
        @test aggregate(gather("movisensxs/data/1.zip", T), T, Week(1)) isa DataFrame
        @test aggregate(gather("movisensxs/data/1.zip", T), T, Day(1)) isa DataFrame
        @test aggregate(gather("movisensxs/data/1.zip", T), T, Hour(12)) isa DataFrame
        @test aggregate(gather("movisensxs/data/1.zip", T), T, Hour(1)) isa DataFrame
    end
end