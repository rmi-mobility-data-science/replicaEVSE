# Notes as I explore the data

## TODO:
* run TNC simuluation -> load_curves
    - only change is public/home l2 logic
* make script to automate the ldv load curves: 
    - sedan, crossover, trucks
    - single, multi-unit
    - does this need anything more than efficiency inputs? make a test?
* loop the years and outputs so we can get this done as fast as possible
* streamline the charges/load outputs squeeze out loadcurves 
(0-24, vs load_kW) then use stackplot to add them together. 
* get matt to figure out charging numbers





### Commercial
* commercial travel in the trips tables have a person_id, however these person_ids are not in the population table. This is fine for PRIVATE_AUTO but will require modification of the code for COMMERCIAL trips. not a lot of modification since we barely use the population table now. We only use it to get the person_ids for people whos distination is in WA. 

### lengths of the full northwest region in millions
* pop_len = 58.222322
* trips_len = 159.624453 
* other_len = 49.674863
* pop_len = 14.889896
* trips_into_wa_len = 51.727268
* private auto trips into wa = 27.12446
* merged_len = 51.727268 
* unique persons who travelled to wa = 8.538399


### Note on vehicle type
* for the northwest region, we only have information on whether
the vehicle type is Heavy or Medium duty trucks. 

### COMMERCIAL
* there is only 1 person per trip in the commercial trips data
* None of the commercial drivers are in the population data
* I don't know how to get `stop_duration` out of trucks...

### Counties
* only LDV have county information from the population table. Commercial Does not. Using destinatino blockgroup to get counties associated with each person_id (trip for commercial)

### Charger availability
* need to include every charge but set the charging value to 0. 

