include("factory_simulation.jl")

# inititialise
seed = 1                # set seed as 1
T = 1000.0              # max time limit
mean_interarrival = 60.0    # interarrival time is exponential mean with 1hr
n_queues = 1
mean_construction_time = 45.0       # construction time is exponential with mean 45mins
mean_interbreakdown_time = 2880.0   # breakdown time is exponential with mean 2 days
mean_repair_time = 180.0            # repair time is exponential with mean 3hrs

time_units = "minutes"              # I used time in mins
P = Parameters( seed, T, n_queues, mean_interarrival, mean_construction_time, mean_interbreakdown_time, mean_repair_time, time_units)

# file directory and name; * concatenates strings.
dir = pwd()*"/data/"*"/seed"*string(P.seed)*"/n_queues"*string(P.n_queues)  # directory name
mkpath(dir)                                                                 # this creates the directory 
file_entities = dir*"/entities.csv"                                         # the name of the entities data file
file_state = dir*"/state.csv"                                               # the name of the state data file
fid_entities = open(file_entities, "w")                                     # open the entities file for writing
fid_state = open(file_state, "w")                                           # open the state file for writing

write_metadata( fid_entities )
write_metadata( fid_state )
write_parameters( fid_entities, P )
write_parameters( fid_state, P )

# headers
write_entity_header( fid_entities,  Entity(0, 0.0) )
print(fid_state,"time,event_id,event_type,length_event_list,length_queue,in_service,machine_status")
print(fid_state)
println(fid_state)

# run the actual simulation
(system,R) = initialise( P ) 
run!( system, P, R, fid_state, fid_entities)

# remember to close the files
close( fid_entities )
close( fid_state )

