
@testset "movisensXS Forms" begin
    Random.seed!(123)

    df = DataFrame(
        :MovisensXSParticipantID => repeat(1:3; inner = 5),
        :FormStart => range(Date("2025-01-01"); length = 15, step = Day(1)) .+
                      rand(Time.(["20:30:00", "21:30:00", "22:00:00"]), 15),
        :HourlyStates => map(x -> JSON.json(Dict("hourStateArray" => x)),
            [rand([0, 1, 2], 24) for _ in 1:15]),
        :Medication => [JSON.json([Dict(
                                       "dose" => rand(["100", "200", "250"]),
                                       "name" => rand(["a", "b", "c"]),
                                       "taken" => rand(0:5),
                                       "type" => 0
                                   ) for _ in 1:rand(0:2)]) for _ in 1:15],
        :StaticLocations => [JSON.json([Dict(
                                            "accuracy" => rand(20:0.5:150),
                                            "id" => ceil(Int, i / 5),
                                            "latitude" => rand(47:01:50),
                                            "longitude" => rand(8:01:10),
                                            "name" => string(rand('a':'z')),
                                            "radius" => 100,
                                            "timestamp" => datetime2unix(DateTime("2025-01-01") +
                                                                         Day(i - 1) +
                                                                         Hour(rand(0:23)) +
                                                                         Minute(rand(0:59)))
                                        ) for _ in 1:rand(0:2)]) for i in 1:15]
    )

    @test ncol(@chain df begin
        groupby(:MovisensXSParticipantID)
        transform([:HourlyStates, :FormStart] => parse(MovisensXSHourlyStates) => :HourlyStates)
    end) == 5

    @test ncol(transform(
        df,
        :Medication => parse(MovisensXSMedication) => [
            :MedicationNames, :MedicationDoses, :MedicationAmounts, :MedicationTypes]
    )) == 9

    @test ncol(transform(
        df,
        :StaticLocations => parse(MovisensXSStaticLocations) => [
            :StaticLocationNames, :StaticLocationDateTimes, :StaticLatitudes,
            :StaticLongitudes, :StaticLocationConfidences, :StaticLocationRadii]
    )) == 11
end