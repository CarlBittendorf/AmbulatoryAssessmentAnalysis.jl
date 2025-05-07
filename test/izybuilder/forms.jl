
@testset "IzyBuilder" begin
    @test nrow(load("izybuilder/data/Example 1.xml", IzyBuilder)) == 4
    @test nrow(load("izybuilder/data/Example 2.xml", IzyBuilder)) == 4
    @test ncol(load("izybuilder/data/Example 1.xml", IzyBuilder)) == 12
    @test ncol(load("izybuilder/data/Example 2.xml", IzyBuilder)) == 12

    @test nrow(gather("izybuilder/data/Example 1.xml", IzyBuilder)) == 4
    @test nrow(gather("izybuilder/data/Example 2.xml", IzyBuilder)) == 4
    @test ncol(gather("izybuilder/data/Example 1.xml", IzyBuilder)) == 12
    @test ncol(gather("izybuilder/data/Example 2.xml", IzyBuilder)) == 12

    @test nrow(gather("izybuilder/data", IzyBuilder)) == 8
    @test ncol(gather("izybuilder/data", IzyBuilder)) == 12
end