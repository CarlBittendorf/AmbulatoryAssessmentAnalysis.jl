
@testset "m-Path Mobile Sensing" begin
    for T in subtypes(MPathMobileSensing)
        # zip files
        @test gather("mpath/data/1/pxaf5_3Kd2ErrJpXkYjwLN.zip", T) isa DataFrame
        @test names(gather("mpath/data/1/pxaf5_3Kd2ErrJpXkYjwLN.zip", T)) ==
              variablenames(T)

        # zip files as Vector{UInt8} or IOBuffer
        @test gather(read("mpath/data/1/pxaf5_5Q899K63kFW2mr4y.zip"), T) isa DataFrame
        @test names(gather(read("mpath/data/1/pxaf5_5Q899K63kFW2mr4y.zip"), T)) ==
              variablenames(T)
        @test gather(IOBuffer(read("mpath/data/1/pxaf5_ALFKDNOFHbtskobU.zip")), T) isa
              DataFrame
        @test names(gather(IOBuffer(read("mpath/data/1/pxaf5_ALFKDNOFHbtskobU.zip")), T)) ==
              variablenames(T)

        # folder containing zip files
        @test gather("mpath/data/1", T) isa DataFrame
        @test names(gather("mpath/data/1", T)) == variablenames(T)
    end
end