# Get time inputs
tList = parse(Int64,ARGS[1]):parse(Int64, ARGS[2])
folder = "./good_model_inputs/";

using DataFrames, CSV, JuMP, CPLEX, Test

# Load model inputs
gen = CSV.read(folder * "inputs_gen_no-nuclear_no_coal_renewables.csv")
load = CSV.read(folder * "inputs_load_no-nuclear.csv")
renCF = CSV.read(folder * "inputs_renewableCF.csv")
trans = CSV.read(folder * "inputs_trans_no-nuclear.csv")

# Define parameters
regions = unique(load[:r])
nTime = length(tList)
nGen = size(gen)[1]
nTrans = size(trans)[1]
transLoss = 0.972

# Define model and variables
m = Model(with_optimizer(CPLEX.Optimizer))

@variable(m, xgen[i=1:nGen, t=1:nTime] >= 0)
@variable(m, xtrans[j=1:nTrans, t=1:nTime] >= 0)

# Constrain transmission
for idx in 1:nTrans
    @constraint(m, xtrans[idx,:] - trans[:transCap][idx] .<= 0)
end

# Constrain generation
for idx in 1:nGen
    if gen[:FuelType][idx] == "Solar" or "solar_generator"
        @constraint(m, xgen[idx,:] - gen[:Capacity][idx] * renCF[tList, :solarCF] .<= 0)
    elseif gen[:FuelType][idx] == "Wind" or "wind_generator"
        # Reduce wind by 15% (calibration)
        @constraint(m, xgen[idx,:] - gen[:Capacity][idx] * renCF[tList, :windCF] * 0.85 .<= 0)
    elseif gen[:FuelType][idx] == "Nuclear"
        @constraint(m, xgen[idx,:] - gen[:Capacity][idx] * 0.95 .<= 0)
    else
        @constraint(m, xgen[idx,:] - gen[:Capacity][idx] .<= 0)
    end
end

# Generation + imports must equal load + exports in each region
for reg in regions
    for t in 1:nTime
        tim = tList[t]
        ld = load[load[:r] .== reg, :demandLoad][tim] # regional load at that time
        @constraint(m, sum(xgen[i,t] for i=1:nGen if gen[:RegionName][i] == reg) +
                        sum(xtrans[j,t] for j=1:nTrans if trans[:r2][j] == reg) * transLoss -
                        sum(xtrans[j,t] for j=1:nTrans if trans[:r1][j] == reg) -
                        ld == 0) # + load
    end
end
                                        
# Minimize generation cost
@objective(m, Min, sum(xgen[i,t] * gen[:FuelCostTotal][i] for i=1:nGen, t=1:nTime) +
                    sum(xtrans[j,t] * trans[:transCost][j] for j=1:nTrans, t=1:nTime))
                                        
JuMP.optimize!(m)
                                                                                
if termination_status(m) == MOI.OPTIMAL
    gen = value.(xgen)
    genOut = convert(DataFrame, gen)
    trans = value.(xtrans)
    transOut = convert(DataFrame, trans)
    optimal_objective = objective_value(m)
    println("Optimal")
    println(optimal_objective)
    CSV.write("./outputs/gen_no-nuclear_no-coal_renewables_$(ARGS[1])_$(ARGS[2]).csv", genOut)
    CSV.write("./outputs/trans_no-nuclear_no-coal_renewables_$(ARGS[1])_$(ARGS[2]).csv", transOut)
elseif termination_status(m) == MOI.TIME_LIMIT && has_values(m)
    suboptimal_gen = value.(xgen)
    genOut = convert(DataFrame, suboptimal_gen)
    suboptimal_trans = value.(xtrans)
    transOut = convert(DataFrame, suboptimal_trans)
    suboptimal_objective = objective_value(m)
    println("Suboptimal")
    println(suboptimal_objective)
    CSV.write("./outputs/subopt_gen_no-nuclear_no-coal_renewables_$(ARGS[1])_$(ARGS[2]).csv", suboptimal_genOut)
    CSV.write("./outputs/subopt_trans_no-nuclear_no-coal_renewables_$(ARGS[1])_$(ARGS[2]).csv", suboptimal_transOut)
else
    error("The model was not solved correctly.")
end
