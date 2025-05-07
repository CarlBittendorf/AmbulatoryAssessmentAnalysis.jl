# AmbulatoryAssessmentAnalysis

[![Aqua](https://raw.githubusercontent.com/JuliaTesting/Aqua.jl/master/badge.svg)](https://github.com/JuliaTesting/Aqua.jl)

## Installation

Follow the instructions on [https://julialang.org/downloads/](https://julialang.org/downloads/) to download and install Julia (if you have not already).

Type `]` in the Julia REPL to enter the package manager REPL mode und run

```
pkg> add https://github.com/CarlBittendorf/AmbulatoryAssessmentAnalysis.jl
```

## Examples

```julia
using AmbulatoryAssessmentAnalysis
using Dates

# replace with your own credentials
studyid = "12345"
apikey = "abcdefghijklmnopqrstuvwxyz"

# download mobile sensing data from movisensXS and save it to "path/to/dir"
download("path/to/dir", MovisensXSUnisens, studyid, apikey)

# load the physical activity data of all participants as a DataFrame
df = gather("path/to/dir", MovisensXSPhysicalActivity)

# calculate aggregated variables at the daily level
aggregate(df, MovisensXSPhysicalActivity, Day(1))
```

## Acknowledgements

Funded by the Deutsche Forschungsgemeinschaft (DFG, German Research Foundation) – GRK2739/1 – Project Nr. 447089431 – Research Training Group: KD²School – Designing Adaptive Systems for Economic Decisions