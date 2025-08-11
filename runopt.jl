# Get time inputs
tList = parse(Int64,ARGS[1]):parse(Int64, ARGS[2])
print(tList)
folder = "./inputs/";

using DataFrames, CSV, JuMP, CPLEX, Test

#run_name = "base"
run_name = "amtk_current"
# Load model inputs
gen = CSV.read(folder * "inputs_gen_$(run_name).csv", DataFrame) #change
load = CSV.read(folder * "inputs_load_$(run_name).csv", DataFrame)
renCF = CSV.read(folder * "inputs_renewableCF.csv", DataFrame)
trans = CSV.read(folder * "inputs_trans_$(run_name).csv", DataFrame)#change

# Define parameters
regions = unique(load[!, :r])
nTime = length(tList)
nGen = size(gen)[1]
nTrans = size(trans)[1]
transLoss = 0.972

# The following did not appear in renewableCF
"""ERC_FRNT
ERC_GWAY
NY_Z_J
SPP_KIAM
"""

# Define model and variables
m = Model(CPLEX.Optimizer)

@variable(m, xgen[i=1:nGen, t=1:nTime] >= 0)
@variable(m, xtrans[j=1:nTrans, t=1:nTime] >= 0)

# Constrain transmission
for idx in 1:nTrans
    for t in 1:nTime
        @constraint(m, xtrans[idx,t] - trans[!,:transCap][idx] .<= 0)
    end
end
println("Check1")
println(size(gen))
println(nGen)
println(nTrans)
println("Check2")

# Constrain generation
for idx in 1:nGen
    for t in 1:nTime
        tim = tList[t]
        if gen[!,:FuelType][idx] == "Solar"
           # print(gen[!,:FuelType][idx]) 
           # print((gen[!,:Capacity][idx]))
            @constraint(m, xgen[idx,t] - gen[!,:Capacity][idx] * renCF[renCF[!,:r].==gen[!,:RegionName][idx], :solarCF][tim] .<= 0)
        elseif gen[!,:FuelType][idx] == "solar_generator"
            #print(gen[!,:FuelType][idx]) 
            #print((gen[!,:Capacity][idx]))
            @constraint(m, xgen[idx,t] - gen[!,:Capacity][idx] * renCF[renCF[!,:r].==gen[!,:RegionName][idx], :solarCF][tim] .<= 0)
        elseif gen[!,:FuelType][idx] == "Wind"
            # Reduce wind by 15% (calibration)
            #print(gen[!,:FuelType][idx]) 
            #print((gen[!,:Capacity][idx]))
            @constraint(m, xgen[idx,t] - gen[!,:Capacity][idx] * renCF[renCF[!,:r].==gen[!,:RegionName][idx], :windCF][tim] * 0.85 .<= 0)
        elseif gen[!,:FuelType][idx] == "wind_generator"
            #print(gen[!,:FuelType][idx]) 
            #print((gen[!,:Capacity][idx]))
            @constraint(m, xgen[idx,t] - gen[!,:Capacity][idx] * renCF[renCF[!,:r].==gen[!,:RegionName][idx], :windCF][tim] * 0.85 .<= 0)
        elseif gen[!,:FuelType][idx] == "Nuclear"
            #print(gen[!,:FuelType][idx]) 
            #print((gen[!,:Capacity][idx]))
            @constraint(m, xgen[idx,t] - gen[!,:Capacity][idx] * 0.95 .<= 0)
        else
            @constraint(m, xgen[idx,t] - gen[!,:Capacity][idx] .<= 0)
        end
    end
end

# Generation + imports must equal load + exports in each region
for reg in regions
    for t in 1:nTime
        tim = tList[t]
        ld = load[load[!,:r] .== reg, :demandLoad][tim] # regional load at that time
        @constraint(m, sum(xgen[i,t] for i=1:nGen if gen[!,:RegionName][i] == reg) +
                        sum(xtrans[j,t] for j=1:nTrans if trans[!,:r2][j] == reg) * transLoss -
                        sum(xtrans[j,t] for j=1:nTrans if trans[!,:r1][j] == reg) -
                        ld == 0) # + load
    end
end
                                        
# Minimize generation cost
@objective(m, Min, sum(xgen[i,t] * gen[!,:FuelCostTotal][i] for i=1:nGen, t=1:nTime) +
                    sum(xtrans[j,t] * trans[!,:transCost][j] for j=1:nTrans, t=1:nTime))
                                        
JuMP.optimize!(m)
                                                                                
if termination_status(m) == MOI.OPTIMAL
    genOut = value.(xgen)
    # genOut = convert(DataFrame, gen)
    transOut = value.(xtrans)
    # transOut = convert(DataFrame, trans)
    optimal_objective = objective_value(m)
    println("Optimal")
    println(optimal_objective)
    CSV.write("./outputs/gen_$(run_name)_$(ARGS[1])_$(ARGS[2]).csv", Tables.table(genOut),writeheader=false)
    CSV.write("./outputs/trans_$(run_name)_$(ARGS[1])_$(ARGS[2]).csv", Tables.table(transOut),writeheader=false)
elseif termination_status(m) == MOI.TIME_LIMIT && has_values(m)
    suboptimal_gen = value.(xgen)
    # genOut = convert(DataFrame, suboptimal_gen)
    suboptimal_trans = value.(xtrans)
    # transOut = convert(DataFrame, suboptimal_trans)
    suboptimal_objective = objective_value(m)
    println("Suboptimal")
    println(suboptimal_objective)
    CSV.write("./outputs/subopt_gen_$(run_name)_$(ARGS[1])_$(ARGS[2]).csv", Tables.table(suboptimal_genOut), writeheader=false)
    CSV.write("./outputs/subopt_trans_$(run_name)_$(ARGS[1])_$(ARGS[2]).csv", Tables.table(suboptimal_transOut),writeheader=false)
else
    error("The model was not solved correctly.")
end
