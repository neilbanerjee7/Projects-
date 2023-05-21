using DataStructures
using Distributions
using StableRNGs
using Printf
using Dates

# Entity data structure for each lawnmower order
mutable struct Entity
    id::Int64
    arrival_time::Float64           # time when the order arrives at the factory
    server::Union{Missing,Int64}    # ID of server where the order is managed
    start_service_time::Float64     # time when the order starts assembly
    completion_time::Float64        # time when the assembly is complete
end

# generate a newly arrived order (where start_service_time and completion_time are unknown)
Entity(id::Int64, arrival_time::Float64 ) = Entity(id, arrival_time, missing, Inf, Inf)

# Events
abstract type Event end 

mutable struct Arrival <: Event     # entity arrives
    id::Int64                       # a unique event id
    time::Float64                   # the time of the event 
end

mutable struct Finish <: Event      # an order finishes processing
    id::Int64                       # a unique event id
    time::Float64                   # the time of the event
    server::Int64                   # ID of the server that finished processing
end

struct Breakdown <: Event   # entity arrives
    id::Int64                       # a unique event id
    time::Float64                   # the time of the event 
end

mutable struct Repair <: Event      # a customer finishes processing at server i
    id::Int64                       # a unique event id
    time::Float64                   # the time of the event
end


struct Null <: Event 
    id::Int64    
end

# parameter structure
struct Parameters
   seed::Int                            # set the seed
   T::Float64                           # max time of running the simulation
   n_queues::Int64                      # dimension of the queue (1 here cause we have only one queue)
   mean_interarrival::Float64           # mean time between arrivals of orders (exponential with mean 1hr)
   mean_construction_time::Float64      # construction time of a lawnmower (deterministic with 45mins)
   mean_interbreakdown_time::Float64    # mean time between breakdown of the machine (exponential with mean 2days)
   mean_repair_time::Float64            # mean repair time of the machine (exponential with mean 3hrs)
   time_units::String                   # units of the time
end

function write_parameters( output::IO, P::Parameters ) # function to write out the parameters
    T = typeof(P)
    for name in fieldnames(T)       # loop to traverse the parameters
        println( output, "# parameter: $name = $(getfield(P,name))" )
    end
end

write_parameters( P::Parameters ) = write_parameters( stdout, P )
function write_metadata( output::IO ) # function to write out the extra metadata
    (path, prog) = splitdir( @__FILE__ )
    println( output, "# file created by code in $(prog)" )
    t = now()
    println( output, "# file created on $(Dates.format(t, "yyyy-mm-dd at HH:MM:SS"))" )
end

# State
mutable struct SystemState
    time::Float64                               # the system time (simulation time)
    n_entities::Int64                           # the number of entities completed served
    n_events::Int64                             # tracks the number of events to have occurred and queued
    event_queue::PriorityQueue{Event,Float64}   # to keep track of future order arrivals
    entity_queues::Array{Queue{Entity},1}       # the system queues
    in_service::Array{Union{Entity,Nothing},1}  # the order currently in assembly if there is one
    machine_status:: Int64                       # check if the machine is broken 
end

function SystemState( P::Parameters )           # create an initial (empty) state
    init_time = 0.0                             # initial time
    init_n_entities = 0                         # no. of initial orders
    init_n_events = 0                           # no. of events initially
    init_event_queue = PriorityQueue{Event,Float64}()               # a blank event queue
    init_entity_queues = Array{Queue{Entity},1}(undef,P.n_queues)   # a blank entity queue
    init_machine_status = 0 
    for i=1:P.n_queues
        init_entity_queues[i] = Queue{Entity}() # populating the queue
    end
    init_in_service = Array{Union{Entity,Nothing},1}(undef,P.n_queues)  # service queue
    for i=1:P.n_queues
        init_in_service[i] = nothing            # populating the queue
    end
    return SystemState( init_time,
                        init_n_entities,
                        init_n_events,
                        init_event_queue,
                        init_entity_queues,
                        init_in_service,
                        init_machine_status)        # return the initial system state
end

# setup random number generators
struct RandNGs
    rng::StableRNGs.LehmerRNG
    interarrival_time::Function
    construction_time::Function
    interbreakdown_time::Function
    repair_time::Function
end

# constructor function to create all the pieces required
function RandNGs( P::Parameters )
    rng = StableRNG( P.seed )                                               # create a new RNG with seed set to that required
    interarrival_time() = rand( rng, Exponential( P.mean_interarrival ) )  
    construction_time() = P.mean_construction_time
    interbreakdown_time() = rand(rng,Exponential(P.mean_interbreakdown_time))
    repair_time() = rand(rng, Exponential(P.mean_repair_time))
    return RandNGs( rng, interarrival_time, construction_time, interbreakdown_time, repair_time)
end

# initialisation function for the simulation
function initialise( P::Parameters )
    # construct random number generators and system state
    R = RandNGs( P )
    system = SystemState( P )

    # add an arrival at time 0.0
    t0 = 0.0
    system.n_events += 1
    enqueue!( system.event_queue, Arrival(0,t0),t0)

    #add a breakdown at time 150.0
    t1 = 150.0
    system.n_events += 1
    enqueue!( system.event_queue, Breakdown(system.n_events, t1 ), t1 )

   return (system, R)
end

# output functions (the formatted output)
function write_state( event_file::IO, system::SystemState, P::Parameters, event::Event, timing::AbstractString, debug_level::Int=0)
    if typeof(event) <: Finish
        type_of_event = "Finish"
    elseif typeof(event) <: Breakdown
            type_of_event = "Breakdown"
    elseif typeof(event) <: Repair
            type_of_event = "Repair"
    else
        type_of_event = typeof(event)
    end
     

 
    @printf(event_file,
            "%12.3f,%6d,%9s,%6d,%4d,%6d,%4d",
            system.time,
            event.id,
            type_of_event,
            length(system.event_queue),
            length(system.entity_queues[1]),
            system.in_service === nothing ? 0 : 1,
            system.machine_status
            )                           # print the event file
            
    @printf(event_file,"\n")
end

function write_entity_header( entity_file::IO, entity )
    T = typeof( entity )
    x = Array{Any,1}(undef, length( fieldnames(typeof(entity)) ) )
    for (i,name) in enumerate(fieldnames(T))
        tmp = getfield(entity,name)
        if isa(tmp, Array)
            x[i] = join( repeat( [name], length(tmp) ), ',' )
        else
            x[i] = name
        end
    end
    println( entity_file, join( x, ',') )
end

function write_entity( entity_file::IO, entity; debug_level::Int=0)
    T = typeof( entity )
    x = Array{Any,1}(undef,length( fieldnames(typeof(entity)) ) )
    for (i,name) in enumerate(fieldnames(T))
        tmp = getfield(entity,name)
        if isa(tmp, Array)
            x[i] = join( tmp, ',' )
        else
            x[i] = tmp
        end
    end
    println( entity_file, join( x, ',') )
end

# Update functions
function update!( system::SystemState, P::Parameters, R::RandNGs, e::Event )
    throw( DomainError("invalid event type" ) )
end

function move_to_server!( system::SystemState, R::RandNGs, server::Integer )
    
    
    # move the order from a queue into construction
    system.in_service[server] = dequeue!(system.entity_queues[server]) 
    system.in_service[server].start_service_time = system.time          # start service time
    completion_time = system.time + R.construction_time()               # total time for the construction of the order
    
    # create a finish event for the order
    system.n_events += 1
    finish_event = Finish( system.n_events, completion_time, server )
    enqueue!( system.event_queue, finish_event, completion_time )
    return nothing
end

function queue_lengths( system::SystemState )
    return length.( system.entity_queues )
end 
 

function in_service( system::SystemState )
    return Int.( system.in_service .!= nothing )
end

function update!( system::SystemState, P::Parameters, R::RandNGs, event::Arrival )
    # create an arriving order and add it to the queue
    system.n_entities += 1    # new entity will enter the system
    new_entity = Entity( system.n_entities, event.time )

    # decide which queue to join based on which is shorter
    lengths = queue_lengths( system ) .+ in_service( system )
    server = argmin( lengths )                              # find the shortest queue + server, and choose lowest index if there is a tie
    new_entity.server = server

    # println("  update arrival: $(queue_lengths( system ))   $(in_service( system ))    $server ")
    
    # add the order to an appropriate queue
    enqueue!(system.entity_queues[server], new_entity)
    
    # generate next arrival and add it to the event queue
    future_arrival = Arrival(system.n_events, system.time + R.interarrival_time())
    enqueue!(system.event_queue, future_arrival, future_arrival.time)

    # if the server is available, order goes to service
    if system.in_service[server] == nothing
        move_to_server!( system, R, server )
    end
    return nothing
end

function update!( S::SystemState, P::Parameters, R::RandNGs, E::Finish)
    server = E.server
    
    departing_entity = deepcopy( system.in_service[server] )
    system.in_service[server] = nothing
        
    if !isempty(system.entity_queues[server])           # if some order is waiting, move them to service
        move_to_server!( system, R, server )
    end
    
    # return the entity when it is leaving the system after assembly
    departing_entity.completion_time = system.time
    return departing_entity
end

function update!( S::SystemState, P::Parameters, R::RandNGs, E::Repair)
    S.machine_status = 0
    S.n_events += 1

    time_breakdown = R.interbreakdown_time()
    current_time = S.time
    breakdown = Breakdown(S.n_events, current_time + time_breakdown)
    enqueue!(S.event_queue, breakdown, current_time + time_breakdown)

    println(S.event_queue)

    if S.in_service == nothing && !S.isempty(entity_queues[1])
        move_to_server!(S, R)
    end
end

function update!( S::SystemState, P::Parameters, R::RandNGs, E::Breakdown)
    S.machine_status = 1 
    S.n_events+= 1  # Increase the no of events by 1
    time_repair = R.repair_time()
    current_time = S.time
    event_repair = Repair(S.n_events,  current_time + time_repair)
    enqueue!(S.event_queue, event_repair, current_time + time_repair)

    if S.in_service !== nothing

        for (event, priority) in S.event_queue
            if typeof(event) == Finish
                S.event_queue[event] += time_repair 
                break
            end
        end
    end
end

function run!( system::SystemState, P::Parameters, R::RandNGs, fid_state::IO, fid_entities::IO; output_level::Integer=2)
    # main simulation loop
    while system.time < P.T
        if P.seed ==1 && system.time <= 1000.0
            println("$(system.time): ")             # debug information for first few events when seed is 1
        end

        # grab the next event from the event queue
        (event, time) = dequeue_pair!(system.event_queue)
        system.time = time                  # advance system time to the new arrival
        system.n_events += 1                # increase the event counter
        
        # write out event and state data before event
        if output_level>=2
            write_state( fid_state, system, P, event, "before")
        elseif output_level==1 && typeof(event) == Arrival
            write_state( fid_state, system, P, event, "before")
        end
        
        # update the system based on the next event, and spawn new events. 
        # return arrived/departed orders
        departure = update!( system, P, R, event )
         
        # write out event and state data after event for debugging
        #if output_level>=2
            #write_state( fid_state, system, P, event, "after")
        #end
        
        #write out entity data if it was a departure from the system
         if departure !== nothing && output_level>=2
            write_entity( fid_entities, departure )
        end
    end
    return system
end


    
