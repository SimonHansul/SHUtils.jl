module SHUtils

using CSV
using DataFrames
using DocStringExtensions

"""
Skip inifite values. 
$(TYPEDSIGNATURES)
"""
skipinf(x::Array) = x[isfinite.(x)]

"""
Parse String to Vector of Floats.
"""
vectify(x) = parse.(Float64, split(split(split(x, "[")[end], "]")[1]," "))


"""
Identify element of `possibilities` occuring in `x`. \\
E.g., which_in("I like apples", ["apples", "bananas"]) returns "apples". \\
If multiple possibilities occur, first detection is returned. 
$(TYPEDSIGNATURES)
"""
function which_in(x::String, possibilities::Vector{String}; none_found_return_val="") 
    idxs = findall(z->occursin(z, x), possibilities)
    if length(idxs)>0
        return possibilities[idxs[1]]
    else
        return none_found_return_val
    end
end


"""
Geometric series created from range of values within a vector.
$(TYPEDSIGNATURES)
"""
geomrange(v::Vector{Float64}; length=50) = 10 .^ range(log10(minimum(v)), log10(maximum(v)), length=length)

"""
Geometric series created from two extreme values.
$(TYPEDSIGNATURES)
"""
geomrange(a::Real, b::Real; length=50) = 10 .^ range(log10(a), log10(b); length=length)

"""
Calculate difference along a vector, inserting NaN as first element (analogous to Pandas' diff method).
$(TYPEDSIGNATURES)
"""
diffvec(x) = vcat([NaN], diff(x))

"""
Formatted rounding to significant digits (omitting decimal point when appropriate). 
Returns rounded number as string.
$(TYPEDSIGNATURES)
"""
function fround(x; sigdigits=2)
    xround = string(round(x, sigdigits=sigdigits))
    if xround[end-1:end]==".0"
        xround = string(xround[1:end-2])
    end
    return xround
end



"""
Drop all missing values from data frame.
"""
function drop_na(df::AbstractDataFrame; verbose=false)
    n0 = nrow(df)
    df2 = df[completecases(df),:]
    dn = nrow(df2)-n0
    if verbose
        @info("Dropped $dn of $n0 rows containing missing values.")
    end
    return df2
end

"""
$(TYPEDSIGNATURES)
"""
function drop_na!(df::DataFrame)
    df = drop_na(df)
end

"""
Replace all missing values in DataFrame with `replace_val`.
$(TYPEDSIGNATURES)
"""
function replace_na!(
    df::AbstractDataFrame,
    cols::Vector{Symbol};
    replace_val = 0.0
    )
    for col in cols
        df[ismissing.(df[:,col]),col] .= replace_val
    end
    return df
end

"""
Infer treatment types (categorical), levels (ordinal) and names (type + level) from Array of exposure concentrations.
$(TYPEDSIGNATURES)
"""
function get_treatment_names(exposure::Vector{Vector{Float64}}, stressor_names::Array{Symbol,1})
    treatment_type = ["co"]
    treatment_level = [0]
    treatment = ["co"]

    treatment_level_counter = 0
    stressor_pre = "co"
    for (i,x) in enumerate(exposure[2:end])
        sum(x.>0) == 1 ? stressor = string(stressor_names[x.>0][1]) : stressor = "mix"
        if stressor == stressor_pre
            treatment_level_counter += 1
        else
            treatment_level_counter = 1
        end
        push!(treatment_type, string(stressor))
        push!(treatment_level, treatment_level_counter)
        push!(treatment, stressor * string(treatment_level_counter))
        stressor_pre = stressor
    end
    return treatment_type, treatment_level, treatment
end


"""
Create legend labels from Vector of numeric Values. 
Kwargs are handed down to fround. 
$(TYPEDSIGNATURES)
"""
function lab(v::Vector{R}; kwargs...) where R <: Real
    return hcat(unique(fround.(v; kwargs...))...)
end

"""
Write DataFrames to disc during loop. Will overwrite existing file if step == 1 and append if step > 1.
$(TYPEDSIGNATURES)
"""
function wrappend(file::String, data::DataFrame, step::Int64)
    if (isfile(file)==false)&(step>1)
        error("Attempt to append to non-existing file: step>1 but file does not exist.")
    end
    if step == 1
        CSV.write(file, data, append=false)
    else
        CSV.write(file, data, append=true)
    end
end

export skipinf, 
vectify,
which_in,
geomrange,
diffvec,
fround,
drop_na,
drop_na!,
replace_na!,
get_treatment_names,
lab

end # module ShUtils
